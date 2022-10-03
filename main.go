package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/Azure/azure-event-hubs-go/v3/persist"
	//"github.com/micro/go-micro/v2/util/ctx"
)

const (
	MAX = 150
	MIN = 100
)

type persistRecord struct {
	namespace     string
	name          string
	consumerGroup string
	partitionID   string
	checkpoint    persist.Checkpoint
}

type batchWriter struct {
	persister 		persist.CheckpointPersister
	dirdata   		string
	writer    		io.Writer
	mu				sync.Mutex
	batchSize      	int
	batch          	[]string
	persistRecords 	[]*persistRecord
	flushed        	*persistRecord
	nextid			int
}

var batchSize = 10

func main() {

	go ProduceEventHub()

	salirdelloop := false
	interrupt := make(chan os.Signal,1)

	// create directory where to keep the last checkpoint
	dirckp, err := persist.NewFilePersister("/tmp/checkpoint")
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	// create directory where to keep the data window
	dirdata := "/tmp/data"
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	
	// set consumer group
	consumerGroup := os.Getenv("EVENTHUB_CONSUMERGROUP")
	if consumerGroup == "" {
		consumerGroup = "$Default"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// create file for a given streaming window
	f, err := os.OpenFile(dirdata+"/"+"pipeline"+strconv.Itoa(1), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
			panic(err)
	}
	defer f.Close()
	
	output, err := NewBatchWriter(dirckp, f, dirdata)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

    
	connStr := "Endpoint=sb://EventHubNameSpace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=xxxxxxxxxxx;EntityPath=Stockmarketsimulator2"
	hub, err := eventhub.NewHubFromConnectionString(connStr)

	if err != nil {
		fmt.Println(err)
		return
	}
	defer hub.Close(ctx)

	eventhub.NewHubFromEnvironment(eventhub.HubWithOffsetPersistence(output))
	
	ehruntime, _ := hub.GetRuntimeInformation(ctx)
	log.Println(ehruntime.Path)
	
	partitions := ehruntime.PartitionIDs
	for _, partitionID := range partitions {
		// receive enters in a for{} loop, receiving msg continuosly
		//_, err := hub.Receive(ctx, partitionID, output.HandleEvent, eventhub.ReceiveWithLatestOffset())
		_, err := hub.Receive(ctx, partitionID, output.HandleEvent, eventhub.ReceiveWithStartingOffset(""))
		if err != nil {
			fmt.Println("Error: ", err)
			return
		}
	}

	ticker := time.NewTicker(2*time.Second)
	for {
		log.Println("primera linea for")
		select {
			case <- ticker.C:
				output.mu.Lock()
				output.Flush(ctx)
				switchFile(output)
				output.mu.Unlock()
				go startPipeline()
			case <- interrupt:
				salirdelloop = true
		}		
		if salirdelloop {
			break
		}
	}
}

func switchFile(o *batchWriter) {
	// create file for a given streaming window
	o.nextid ++
	f, err := os.OpenFile(o.dirdata+"/"+"pipeline"+strconv.Itoa(o.nextid), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0600)
    if err != nil {
        panic(err)
    }
    defer f.Close()
}

func startPipeline() {
	//savefile()
	log.Println(" start Pipeline")
	//startpipeline()
}


// NewBatchWriter creates an object that can be used as both a `persist.CheckpointPersister` and an Event Hubs Event Handler `batchWriter.HandleEvent`
func NewBatchWriter(persister persist.CheckpointPersister, writer io.Writer, dirdata string) (*batchWriter, error) {
	return &batchWriter{
		persister:      persister,
		dirdata: 		dirdata,
		writer:         writer,
		batchSize:      batchSize,
		batch:          make([]string, 0, batchSize),
		persistRecords: make([]*persistRecord, 0, batchSize),
		nextid:			1,
	}, nil
}

// Read reads the last checkpoint
func (w *batchWriter) Read(namespace, name, consumerGroup, partitionID string) (persist.Checkpoint, error) {
	return w.persister.Read(namespace, name, consumerGroup, partitionID)
}

// Write will write the last checkpoint of the last event flushed and record persist records for future use
func (w *batchWriter) Write(namespace, name, consumerGroup, partitionID string, checkpoint persist.Checkpoint) error {
	var err error
	if w.flushed != nil {	
		r := w.flushed
		err = w.persister.Write(r.namespace, r.name, r.consumerGroup, r.partitionID, r.checkpoint)
		if err != nil {
			w.flushed = nil
		}
	}
	w.persistRecords = append(w.persistRecords, &persistRecord{
		namespace:     namespace,
		name:          name,
		consumerGroup: consumerGroup,
		partitionID:   partitionID,
		checkpoint:    checkpoint,
	})
	return err
}
// HandleEvent will handle Event Hubs Events
// If the length of the batch buffer has reached the max batchSize, the buffer will be flushed before appending the new event
// If flush fails and it hasn't made space in the buffer, the flush error will be returned to the caller
func (w *batchWriter) HandleEvent(ctx context.Context, event *eventhub.Event) error {
	w.mu.Lock()
	if len(w.batch) >= batchSize {
		err := w.Flush(ctx)
		// If we received an error flushing and still don't have room in the buffer return the error
		if err != nil && len(w.batch) >= batchSize {
			return err
		}
	}
	// Append the event to the buffer if we have room for it
	w.batch  = append(w.batch, string(event.Data))
	w.mu.Unlock()
	return nil
}

// Flush flushes the buffer to the given io.Writer
// Post-condition:
//   error == nil: buffer has been flushed successfully, buffer has been replaced with a new buffer
//   error != nil: some or no events have been flushed, buffer contains only events that failed to flush
func (w *batchWriter) Flush(ctx context.Context) error {
	for i, s := range w.batch {
		_, err := fmt.Fprintln(w.writer, s)
		if err != nil {
			w.batch  = w.batch[i:]
			w.persistRecords = w.persistRecords[i:]
			return err
		}
		w.flushed = w.persistRecords[i]
	}
	w.batch  = make([]string, 0, batchSize)
	w.persistRecords = make([]*persistRecord, 0, batchSize)
	return nil
}

func ProduceEventHub() {
	connStr := "Endpoint=sb://EventHubNameSpaceCNM.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=VjOiSq7sAloTVHayp/g2kVt5EbVQYblWa8ymRyvBBxE=;EntityPath=Stockmarketsimulator2"
	hub, err := eventhub.NewHubFromConnectionString(connStr)

	if err != nil {
		fmt.Println(err)	
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// send intraday values to Azure Eventhub for GOOG
	for k := 1; k < 101; k++ {
		rand.Seed(time.Now().UnixNano())

		r1 := rand.Intn(MAX-MIN) + MIN
		r2 := rand.Intn(MAX-MIN) + MIN
		if r1 > r2 {
			r1, r2 = r2, r1
		}
		t := time.Now()
		formatted := fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
		fmt.Println(formatted)

		err = hub.Send(ctx, eventhub.NewEventFromString(formatted))
		if err != nil {
			fmt.Println(err)
			return
		}
	}

	err = hub.Close(context.Background())
	if err != nil {
		fmt.Println(err)
	}

}