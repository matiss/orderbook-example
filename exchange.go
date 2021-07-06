package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/matiss/orderbook"
)

const (
	binaceAPIBaseURL         = "https://api.binance.com"
	binanceWsBaseURL         = "wss://stream.binance.com:9443/ws"
	binanceWsBaseFutureURL   = "wss://fstream.binance.com/ws"
	binanceWSCombinedBaseURL = "wss://stream.binance.com:9443/stream?streams="
	binanceWSPongWait        = (60 * time.Second)
	binanceWSmaxMessageSize  = 8192
	binanceReceiveWindow     = "5000"
)

// StreamDepthHandler type
type StreamDepthHandler func(event *orderbook.DepthEvent)

// StreamHandler handle raw websocket message
type StreamHandler func(message []byte)

// StreamErrHandler handles errors
type StreamErrHandler func(err error)

// Exchange exchange struct
type Exchange struct {
	name   string
	key    string
	secret string
}

// newExchange creates new Binance struct
func NewExchange(key, secret string) *Exchange {
	return &Exchange{
		name:   "binance",
		key:    key,
		secret: secret,
	}
}

// Name for exchange
func (e *Exchange) Name() string {
	return e.name
}

// Fees for exchange
func (e *Exchange) Fees() (float64, float64) {
	return 0, 0
}

// GetAvailable funds
func (e *Exchange) GetAvailable() int64 {
	return 0
}

// Ping Binace api server
func (e *Exchange) Ping() (int, error) {

	start := time.Now()

	res, err := e.request("GET", "api/v3/ping", map[string]string{}, false, false)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()

	duration := time.Since(start).Milliseconds()

	return int(duration), nil
}

// DepthStream opens new connection
func (e *Exchange) DepthStream(symbolLevels map[string]string, handler StreamDepthHandler, errorHandler StreamErrHandler) (chan struct{}, chan struct{}, error) {
	// Websocket endpoint
	endpoint := binanceWSCombinedBaseURL

	for s, l := range symbolLevels {
		endpoint += fmt.Sprintf("%s@depth%s", strings.ToLower(s), l) + "/"
	}

	endpoint = endpoint[:len(endpoint)-1]

	// Websocket connection
	conn, _, err := websocket.DefaultDialer.Dial(endpoint, nil)
	if err != nil {
		return nil, nil, err
	}

	// Stop channel
	stopC := make(chan struct{})
	doneC := make(chan struct{})

	go func() {
		defer close(doneC)

		// Keepalive
		// keepAlive(conn, binanceWSPongWait)

		silent := false
		go func() {
			select {
			case <-stopC:
				// silent = true
				err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				if err != nil {
					errorHandler(err)
					return
				}

				time.Sleep(time.Second)
			case <-doneC:
			}

			// Close connection
			conn.Close()
		}()

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				if !silent {
					errorHandler(err)
				}
				break
			}

			event, err := e.getDepthEvent(message)
			if err != nil {
				errorHandler(err)
			}

			// Handle stream event
			handler(event)
		}

	}()

	return doneC, stopC, nil
}

type binancePartialDepthEventData struct {
	EventType     string     `json:"e"`
	EventTime     uint64     `json:"E"`
	Symbol        string     `json:"s"`
	FirstUpdateID int64      `json:"U"`
	FinalUpdateID int64      `json:"u"`
	Asks          [][]string `json:"a"`
	Bids          [][]string `json:"b"`
}

type binancePartialDepthEvent struct {
	Stream string                       `json:"stream"`
	Time   uint64                       `json:"time"`
	Data   binancePartialDepthEventData `json:"data"`
}

func (e *Exchange) getDepthEvent(msg []byte) (*orderbook.DepthEvent, error) {
	bEvent := binancePartialDepthEvent{}

	err := json.Unmarshal(msg, &bEvent)
	if err != nil {
		return nil, err
	}

	event := &orderbook.DepthEvent{
		Symbol:        bEvent.Data.Symbol,
		FirstUpdateID: bEvent.Data.FirstUpdateID,
		FinalUpdateID: bEvent.Data.FinalUpdateID,
		Timestamp:     time.Now(),
	}

	event.Asks = make([]*orderbook.Ask, len(bEvent.Data.Asks))
	event.Bids = make([]*orderbook.Bid, len(bEvent.Data.Bids))

	var quantity int64 = 0

	for i := 0; i < len(bEvent.Data.Asks); i++ {
		quantity = orderbook.DecimalStringToSize(bEvent.Data.Asks[i][1])

		event.Asks[i] = &orderbook.Ask{
			Price:    orderbook.DecimalStringToSatoshi(bEvent.Data.Asks[i][0]),
			Quantity: quantity,
			Delete:   (quantity == 0),
		}
	}

	for i := 0; i < len(bEvent.Data.Bids); i++ {
		quantity = orderbook.DecimalStringToSize(bEvent.Data.Bids[i][1])

		event.Bids[i] = &orderbook.Bid{
			Price:    orderbook.DecimalStringToSatoshi(bEvent.Data.Bids[i][0]),
			Quantity: quantity,
			Delete:   (quantity == 0),
		}
	}

	return event, nil
}

type binanceDepthSnapshot struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Asks         [][]string `json:"asks"`
	Bids         [][]string `json:"bids"`
}

// GetDepthSnapshot from exchange
func (e *Exchange) GetDepthSnapshot(symbol string, limit int) (*orderbook.DepthSnapshot, error) {
	snapshot := orderbook.DepthSnapshot{}
	bSnapshot := binanceDepthSnapshot{}

	// Validate symbol
	if len(symbol) < 3 {
		return nil, errors.New("invalid symbol")
	}

	symbol = strings.ToUpper(symbol)

	// Limit constraints
	limitR := limit
	if limitR <= 0 {
		limitR = 100
	} else if limitR > 1000 {
		limitR = 1000
	}

	// Add query params
	params := make(map[string]string)
	params["symbol"] = symbol
	params["limit"] = strconv.FormatInt(int64(limitR), 10)

	res, err := e.request("GET", "api/v3/depth", params, false, false)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	err = json.NewDecoder(res.Body).Decode(&bSnapshot)
	if err != nil {
		return nil, err
	}

	// Process snapshot
	snapshot.LastUpdateID = bSnapshot.LastUpdateID

	snapshot.Asks = make([]*orderbook.Ask, len(bSnapshot.Asks))
	snapshot.Bids = make([]*orderbook.Bid, len(bSnapshot.Bids))

	// Asks
	for i := 0; i < len(bSnapshot.Asks); i++ {
		snapshot.Asks[i] = &orderbook.Ask{
			Price:    orderbook.DecimalStringToSatoshi(bSnapshot.Asks[i][0]),
			Quantity: orderbook.DecimalStringToSize(bSnapshot.Asks[i][1]),
			Delete:   false,
		}
	}

	// Bids
	for i := 0; i < len(bSnapshot.Bids); i++ {
		snapshot.Bids[i] = &orderbook.Bid{
			Price:    orderbook.DecimalStringToSatoshi(bSnapshot.Bids[i][0]),
			Quantity: orderbook.DecimalStringToSize(bSnapshot.Bids[i][1]),
			Delete:   false,
		}
	}

	return &snapshot, nil
}

func (e *Exchange) request(method string, endpoint string, params map[string]string,
	apiKey bool, sign bool) (*http.Response, error) {
	transport := &http.Transport{}
	client := &http.Client{
		Transport: transport,
		Timeout:   time.Duration(time.Second * 5),
	}

	url := fmt.Sprintf("%s/%s", binaceAPIBaseURL, endpoint)
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return nil, err
	}

	// With Context
	// req.WithContext(as.Ctx)

	q := req.URL.Query()
	for key, val := range params {
		q.Add(key, val)
	}

	// Add key
	if apiKey {
		req.Header.Add("X-MBX-APIKEY", e.key)
	}

	// Sign
	if sign {
		raw := q.Encode()
		mac := hmac.New(sha256.New, []byte(e.secret))

		// Add payload
		_, err := mac.Write([]byte(raw))
		if err != nil {
			return nil, err
		}

		// Append signature to the query string
		q.Add("signature", hex.EncodeToString(mac.Sum(nil)))
	}

	// Add query string to the URL
	req.URL.RawQuery = q.Encode()

	// Make request
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// Close exchange
func (e *Exchange) Close() {

}
