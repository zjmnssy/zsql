package zsql

import (
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// sql event type.
const (
	EventTypeOfRows = "rows"
	EventTypeOfTx   = "tx"

	EventOperateOfnNew  = "new"
	EventOperateOfClose = "close"

	defaultEventManagerPeriod  = 60
	defaultEventManagerTimeout = 180

	defaultEventChannelLen = 100 * 10000
)

// EventItem sql event item.
type EventItem struct {
	Type    string
	Operate string
	Rows    *sql.Rows
	Tx      *sql.Tx
	Desc    string

	timeBegin int64
	timeDiff  int
}

// Manager sql event manager.
type Manager struct {
	period  time.Duration
	timeout time.Duration
	RowMap  sync.Map
	TxMap   sync.Map

	eventCh chan EventItem
}

// AddEvent add a sql event.
func (m *Manager) AddEvent(event EventItem) {
	t := time.Now()
	event.timeBegin = t.Unix()

	m.eventCh <- event
}

// SetConfig set sql manager config.
func (m *Manager) SetConfig(p time.Duration, t time.Duration) {
	m.period = p
	m.timeout = t
}

func (m *Manager) manage() {
	for {
		event := <-m.eventCh

		if event.Type == EventTypeOfRows {
			if event.Operate == EventOperateOfnNew {
				_, ok := m.RowMap.LoadOrStore(event.Rows, event)
				if ok {
					fmt.Printf("query event key exist = %v\n", event)
				}
			} else if event.Operate == EventOperateOfClose {
				m.RowMap.Delete(event.Rows)
			} else {
				fmt.Printf("unknown event.Operate = %s\n", event.Operate)
			}
		} else if event.Type == EventTypeOfTx {
			if event.Operate == EventOperateOfnNew {
				_, ok := m.RowMap.LoadOrStore(event.Tx, event)
				if ok {
					fmt.Printf("tx event key exist = %v\n", event)
				}
			} else if event.Operate == EventOperateOfClose {
				m.RowMap.Delete(event.Tx)
			} else {
				fmt.Printf("unknown event.Operate = %s\n", event.Operate)
			}
		} else {
			fmt.Printf("unknown event.Type = %s\n", event.Type)
		}
	}
}

func (m *Manager) scan() {
	rowF := func(k, v interface{}) bool {
		timeNow := time.Now().Unix()

		if (timeNow - v.(EventItem).timeBegin) > int64(m.timeout) {
			fmt.Printf("rows timeout %d second to close, event = %v\n",
				int(timeNow-v.(EventItem).timeBegin), v.(EventItem))

			if v.(EventItem).Rows == nil {
				return true
			}

			e := v.(EventItem).Rows.Close()
			if e != nil {
				fmt.Printf("rows of %v close error = %s\n", v.(EventItem), e)
			}

			m.RowMap.Delete(k)
		}

		return true
	}

	m.RowMap.Range(rowF)

	txF := func(k, v interface{}) bool {
		timeNow := time.Now().Unix()

		if (timeNow - v.(EventItem).timeBegin) > int64(m.timeout) {
			fmt.Printf("tx timeout %d second to close, event = %v\n",
				int(timeNow-v.(EventItem).timeBegin), v.(EventItem))

			if v.(EventItem).Tx == nil {
				return true
			}

			e := v.(EventItem).Tx.Rollback()
			if e != nil {
				fmt.Printf("tx of %v rollback error = %s\n", v.(EventItem), e)
			}

			m.TxMap.Delete(k)
		}

		return true
	}

	m.TxMap.Range(txF)
}

func (m *Manager) run() {
	timer := time.NewTimer(time.Second * m.period)

	m.scan()

	for {
		select {
		case <-timer.C:
			{
				m.scan()
				timer.Reset(time.Second * m.period)
			}
		}
	}
}

func newManager() *Manager {
	m := &Manager{period: 180, timeout: 300, eventCh: make(chan EventItem, defaultEventChannelLen)}

	go m.run()
	go m.manage()

	return m
}

var once sync.Once
var instance *Manager

// GetManager get sql event manager.
func GetManager() *Manager {
	once.Do(func() {
		instance = newManager()
	})

	return instance
}
