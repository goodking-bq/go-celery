package message

import (
	"encoding/json"
	"fmt"
	uuid "github.com/satori/go.uuid"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"
)

// CeleryMessagePropertiesDeliveryInfo represents deliveryinfo json
type CeleryMessagePropertiesDeliveryInfo struct {
	RoutingKey string `json:"routing_key"`
	Exchange   string `json:"exchange"`
}

// CeleryMessageProperties represents properties json
type CeleryMessageProperties struct {
	BodyEncoding  string                              `json:"body_encoding"`
	CorrelationID string                              `json:"correlation_id"`
	ReplyTo       string                              `json:"reply_to"`
	DeliveryInfo  CeleryMessagePropertiesDeliveryInfo `json:"delivery_info"`
	DeliveryMode  int                                 `json:"delivery_mode"`
	DeliveryTag   string                              `json:"delivery_tag"`
	Priority      uint8                               `json:"priority"`
}

// CeleryMessage is actual message to be sent to Redis
type CeleryMessage struct {
	Body            string                  `json:"body"`
	Headers         map[string]interface{}  `json:"headers,omitempty"`
	ContentType     string                  `json:"content-type"`
	Properties      CeleryMessageProperties `json:"properties"`
	ContentEncoding string                  `json:"content-encoding"`
	Queue           string                  `json:"-"`
}

var celeryMessagePool = sync.Pool{
	New: func() interface{} {
		return &CeleryMessage{
			Body:        "",
			Headers:     nil,
			ContentType: "application/json",
			Properties: CeleryMessageProperties{
				BodyEncoding:  "base64",
				CorrelationID: uuid.Must(uuid.NewV4()).String(),
				ReplyTo:       uuid.Must(uuid.NewV4()).String(),
				DeliveryInfo: CeleryMessagePropertiesDeliveryInfo{
					RoutingKey: "celery",
					Exchange:   "celery",
				},
				DeliveryMode: 2,
				DeliveryTag:  uuid.Must(uuid.NewV4()).String(),
				Priority:     0,
			},
			ContentEncoding: "utf-8",
		}
	},
}

func GetCeleryMessage(encodedTaskMessage string) *CeleryMessage {
	msg := celeryMessagePool.Get().(*CeleryMessage)
	msg.Body = encodedTaskMessage
	return msg
}
func (cm *CeleryMessage) reset() {
	cm.Headers = nil
	cm.Body = ""
	cm.Properties.CorrelationID = uuid.Must(uuid.NewV4()).String()
	cm.Properties.ReplyTo = uuid.Must(uuid.NewV4()).String()
	cm.Properties.DeliveryTag = uuid.Must(uuid.NewV4()).String()
}

func ReleaseCeleryMessage(v *CeleryMessage) {
	v.reset()
	celeryMessagePool.Put(v)
}

func (cm CeleryMessage) GetTaskMessage() *TaskMessage {
	// ensure content-type is 'application/json'
	if cm.ContentType != "application/json" {
		log.Println("unsupported content type " + cm.ContentType)
		return nil
	}
	// ensure body encoding is base64
	if cm.Properties.BodyEncoding != "base64" {
		log.Println("unsupported body encoding " + cm.Properties.BodyEncoding)
		return nil
	}
	// ensure content encoding is utf-8
	if cm.ContentEncoding != "utf-8" {
		log.Println("unsupported encoding " + cm.ContentEncoding)
		return nil
	}
	taskMessage := GetTaskMessage()
	var decodeFun func(body string, message *TaskMessage) error
	if len(cm.Headers) == 0 {
		decodeFun = DecodeMessageBodyV1
	} else {
		decodeFun = DecodeMessageBodyV2
		b, _ := json.Marshal(cm.Headers)
		_ = json.Unmarshal(b, taskMessage)
	}
	if err := decodeFun(cm.Body, taskMessage); err != nil {
		log.Println("failed to decode task message")
		return nil
	}
	return taskMessage

}

// DecodeMessageBodyV1 decodes base64 encrypted body and return TaskMessage object for celery protocol 1
func DecodeMessageBodyV1(encodedBody string, msg *TaskMessage) error {
	err := json.Unmarshal([]byte(encodedBody), msg)
	if err != nil {
		return err
	}
	return nil
}

func EncodeMessageBodyV1(task *TaskMessage) (body string, err error) {
	return "", nil
}

// DecodeMessageBodyV2 decodes base64 encrypted body and return TaskMessage object for celery protocol 2
func DecodeMessageBodyV2(encodedBody string, msg *TaskMessage) error {
	var bodyTemp []interface{}
	if err := json.Unmarshal([]byte(encodedBody), &bodyTemp); err != nil {
		return err
	}
	//info := bodyTemp[2].(map[string]interface{})
	msg.Args = bodyTemp[0].([]interface{})
	msg.Kwargs = bodyTemp[1].(map[string]interface{})
	return nil
}

func EncodeMessageBodyV2(task *TaskMessage) (body string, err error) {
	b := make([]interface{}, 3)
	b[0] = task.Args
	b[1] = task.Kwargs
	b[2] = map[string]interface{}{
		"callbacks": nil,
		"errbacks":  nil,
		"chain":     nil,
		"chord":     nil,
	}
	bByte, err := json.Marshal(b)
	if err != nil {
		return "", err
	}
	//bStr := base64.StdEncoding.EncodeToString(bByte)
	return string(bByte), nil
}

// CeleryTime covert time
type CeleryTime struct {
	time.Time
}

func (date CeleryTime) MarshalJSON() ([]byte, error) {
	f := date.Format("2006-01-02T15:04:05.000000")
	return []byte(`"` + f + `"`), nil
}

func (date *CeleryTime) UnmarshalJSON(b []byte) error {
	format := "2006-01-02T15:04:05"
	ciphertext := strings.Replace(string(b), "\"", "", -1)
	t, err := time.Parse(format, ciphertext)
	if err != nil {
		return err
	}
	*date = CeleryTime{t}
	return nil
}

type CeleryResultMessage struct {
	Status    string        `json:"status"`
	Result    interface{}   `json:"result"`
	Traceback interface{}   `json:"traceback"`
	Children  []interface{} `json:"children"`
	DateDone  CeleryTime    `json:"date_done"`
	TaskId    string        `json:"task_id"`
}

func (rm *CeleryResultMessage) reset() {
	rm.Result = nil
}

var resultMessagePool = sync.Pool{
	New: func() interface{} {
		return &CeleryResultMessage{
			Status:    "SUCCESS",
			Traceback: nil,
			Children:  []interface{}{},
		}
	},
}

func GetResultMessage(val interface{}) *CeleryResultMessage {
	msg := resultMessagePool.Get().(*CeleryResultMessage)
	msg.Result = val
	msg.DateDone = CeleryTime{time.Now()}
	return msg
}

func GetReflectionResultMessage(val *reflect.Value) *CeleryResultMessage {
	msg := resultMessagePool.Get().(*CeleryResultMessage)
	msg.Result = GetRealValue(val)
	msg.DateDone = CeleryTime{time.Now()}
	return msg
}

func ReleaseResultMessage(v *CeleryResultMessage) {
	v.reset()
	resultMessagePool.Put(v)
}

// Backend is interface for celery backend database
type Backend interface {
	GetResult(string) (*CeleryResultMessage, error) // must be non-blocking
	SetResult(taskID string, result *CeleryResultMessage) error
}

// AsyncResult represents pending result
type AsyncResult struct {
	TaskID  string
	Backend Backend
	result  *CeleryResultMessage
}

// Wait wait task return in timeout duration
func (r *AsyncResult) Wait(timeout time.Duration) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	timeoutChan := time.After(timeout)
	for {
		select {
		case <-timeoutChan:
			err := fmt.Errorf("%v timeout getting result for %s", timeout, r.TaskID)
			return err
		case <-ticker.C:
			err := r.asyncResult()
			if err != nil {
				continue
			}
			return nil
		}
	}
}

func (r *AsyncResult) asyncResult() error {
	if r.result != nil {
		return nil
	}
	val, err := r.Backend.GetResult(r.TaskID)
	if err != nil || val == nil {
		return err
	}
	//if val.Status != "SUCCESS" {
	//	return fmt.Errorf("error response status %v", val)
	//}
	r.result = val
	return nil
}

// Get get task result within timeout
func (r *AsyncResult) Get(timeout time.Duration) (interface{}, error) {
	err := r.Wait(timeout)
	if err != nil {
		return nil, err
	}
	return r.result.Result, nil
}

func (r *AsyncResult) Successful() bool {
	if r.result == nil {
		return false
	}
	if r.result.Status == "SUCCESS" {
		return true
	}
	return false
}

func (r *AsyncResult) Result() interface{} {
	return r.result.Result
}

// TaskMessage is celery-compatible message
type TaskMessage struct {
	ID      string                 `json:"id"`
	Task    string                 `json:"task"`
	Args    []interface{}          `json:"args"`
	Kwargs  map[string]interface{} `json:"kwargs"`
	Retries int                    `json:"retries"`
	ETA     *string                `json:"eta"`
	Expires *CeleryTime            `json:"expires"`
	Lang    string                 `json:"lang"`
}

func (tm *TaskMessage) reset() {
	tm.ID = uuid.Must(uuid.NewV4()).String()
	tm.Task = ""
	tm.Args = nil
	tm.Kwargs = nil
}

func (tm *TaskMessage) ToHeader() (res map[string]interface{}) {
	b, _ := json.Marshal(tm)
	_ = json.Unmarshal(b, &res)
	return
}

var taskMessagePool = sync.Pool{
	New: func() interface{} {
		eta := time.Now().Format(time.RFC3339)
		return &TaskMessage{
			ID:      uuid.Must(uuid.NewV4()).String(),
			Retries: 0,
			Kwargs:  nil,
			ETA:     &eta,
			Lang:    "golang",
		}
	},
}

func GetTaskMessage() *TaskMessage {
	msg := taskMessagePool.Get().(*TaskMessage)
	msg.Task = ""
	msg.Args = make([]interface{}, 0)
	msg.Kwargs = make(map[string]interface{})
	msg.ETA = nil
	return msg
}

func ReleaseTaskMessage(v *TaskMessage) {
	v.reset()
	taskMessagePool.Put(v)
}
