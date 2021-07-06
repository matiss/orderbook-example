package main

import (
	"log"
	"strings"
	"sync"
	"time"

	"github.com/matiss/orderbook"
)

// Market symbol
const symbol = "btcusdt"

// Orderbook initial depth
const orderbookDepth = 20

// Orderbook depth limit
const orderbookPruneDepth = 50

var mu *sync.Mutex
var orderBooks map[string]*orderbook.OrderBook
var eventBuffer map[string][]*orderbook.DepthEvent
var eventQueue chan *orderbook.DepthEvent

// addDepthEvent adds market depth event
func addDepthEvent(event *orderbook.DepthEvent) error {
	// Lock
	mu.Lock()
	defer mu.Unlock()

	if _, ok := orderBooks[event.Symbol]; ok {
		return orderBooks[event.Symbol].ProcessEvent(event)
	}

	// Add event to the buffer
	eventBuffer[event.Symbol] = append(eventBuffer[event.Symbol], event)

	return nil
}

// addOrderBookSnapshot snapshot to the cache
func addOrderBookSnapshot(symbol string, snapshot *orderbook.DepthSnapshot, pruneThreshold int) {
	// Lock
	mu.Lock()
	defer mu.Unlock()

	sym := strings.ToUpper(symbol)

	// Se default threshold
	if pruneThreshold < 100 {
		pruneThreshold = 100
	}

	orderBooks[sym] = orderbook.NewOrderBook(sym, &orderbook.OrderBookList{}, &orderbook.OrderBookList{}, pruneThreshold)

	// Add snapshot
	orderBooks[sym].ProcessSnapshot(snapshot, eventBuffer[sym])

	// Clear event buffer
	eventBuffer[sym] = make([]*orderbook.DepthEvent, 0)
}

func depthHandler(event *orderbook.DepthEvent) {
	// Add event to the event queue
	eventQueue <- event
}

func streamErrorHandler(err error) {
	// Print error
	log.Println(err)
}

func processDepthEvents() {
	for event := range eventQueue {
		err := addDepthEvent(event)
		if err != nil {
			log.Println(err)
		}
	}
}

func printPrice() {
	market := strings.ToUpper(symbol)

	for {
		if _, ok := orderBooks[market]; ok {
			price, _ := orderBooks[market].GetMarketPrice()

			// Print price
			log.Printf("%s: %s\n", market, price.StringFixed(3))
		}

		// Sleep for one second
		time.Sleep(250 * time.Millisecond)
	}
}

func init() {
	eventQueue = make(chan *orderbook.DepthEvent, 10)
	orderBooks = make(map[string]*orderbook.OrderBook)
	eventBuffer = make(map[string][]*orderbook.DepthEvent)
	mu = new(sync.Mutex)
}

func main() {
	// Map market symbols
	symbolLevels := make(map[string]string, 1)
	symbolLevels[symbol] = "@100ms"

	// Process depth events
	go processDepthEvents()

	// Setup exchange
	exchange := NewExchange("", "")

	// Start websocket stream
	depthStreamDoneC, _, err := exchange.DepthStream(symbolLevels, depthHandler, streamErrorHandler)
	if err != nil {
		streamErrorHandler(err)
	}

	// Get latest depth snapshot (orderbook)
	snapshot, err := exchange.GetDepthSnapshot(symbol, orderbookDepth)
	if err != nil {
		log.Fatal(err)
	} else {
		addOrderBookSnapshot(symbol, snapshot, orderbookPruneDepth)
	}

	go printPrice()

	log.Println("Ready!")

	// Block loop as long as stream is active
	<-depthStreamDoneC
}
