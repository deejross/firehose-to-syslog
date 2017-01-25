package events

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/deejross/firehose-to-syslog/caching"
	log "github.com/deejross/firehose-to-syslog/logging"
	"github.com/deejross/firehose-to-syslog/utils"
)

type Event struct {
	Fields logrus.Fields
	Msg    string
	Type   string
}

var selectedEvents map[string]bool
var selectedEventsCount map[string]uint64 = make(map[string]uint64)
var mutex sync.Mutex
var logMessageLimit int
var silentTime time.Duration

func RouteEvents(in <-chan *events.Envelope, extraFields map[string]string) {
	for msg := range in {
		routeEvent(msg, extraFields)
	}
}

func GetSelectedEvents() map[string]bool {
	return selectedEvents
}

func routeEvent(msg *events.Envelope, extraFields map[string]string) {

	eventType := msg.GetEventType()

	if selectedEvents[eventType.String()] {
		var event Event
		switch eventType {
		case events.Envelope_HttpStart:
			event = HttpStart(msg)
		case events.Envelope_HttpStop:
			event = HttpStop(msg)
		case events.Envelope_HttpStartStop:
			event = HttpStartStop(msg)
		case events.Envelope_LogMessage:
			event = LogMessage(msg)
		case events.Envelope_ValueMetric:
			event = ValueMetric(msg)
		case events.Envelope_CounterEvent:
			event = CounterEvent(msg)
		case events.Envelope_Error:
			event = ErrorEvent(msg)
		case events.Envelope_ContainerMetric:
			event = ContainerMetric(msg)
		}

		event.AnnotateWithEnveloppeData(msg)

		if _, hasAppId := event.Fields["cf_app_id"]; hasAppId {
			event.AnnotateWithAppData()
		}

		event.AnnotateWithMetaData(extraFields)
		event.ShipEvent()

		mutex.Lock()
		selectedEventsCount[eventType.String()]++
		mutex.Unlock()
	}
}

func SetupEventRouting(wantedEvents string) error {
	selectedEvents = make(map[string]bool)

	if wantedEvents == "" {
		selectedEvents["LogMessage"] = true
	} else {
		for _, event := range strings.Split(wantedEvents, ",") {
			if isAuthorizedEvent(strings.TrimSpace(event)) {
				selectedEvents[strings.TrimSpace(event)] = true
				log.LogStd(fmt.Sprintf("Event Type [%s] is included in the fireshose!", event), false)
			} else {
				return fmt.Errorf("Rejected Event Name [%s] - Valid events: %s", event, GetListAuthorizedEventEvents())
			}
		}
	}
	return nil
}

func isAuthorizedEvent(wantedEvent string) bool {
	for _, authorizeEvent := range events.Envelope_EventType_name {
		if wantedEvent == authorizeEvent {
			return true
		}
	}
	return false
}

func GetListAuthorizedEventEvents() (authorizedEvents string) {
	arrEvents := []string{}
	for _, listEvent := range events.Envelope_EventType_name {
		arrEvents = append(arrEvents, listEvent)
	}
	return strings.Join(arrEvents, ", ")
}

func GetTotalCountOfSelectedEvents() uint64 {
	var total = uint64(0)
	for _, count := range GetSelectedEventsCount() {
		total += count
	}
	return total
}

func GetSelectedEventsCount() map[string]uint64 {
	mutex.Lock()
	defer mutex.Unlock()
	return selectedEventsCount
}

func getAppInfo(appGuid string) caching.App {
	if app := caching.GetAppInfo(appGuid); app.Name != "" {
		return app
	} else {
		caching.GetAppByGuid(appGuid)
	}
	return caching.GetAppInfo(appGuid)
}

func (e *Event) AnnotateWithEnveloppeData(msg *events.Envelope) {
	e.Fields["origin"] = msg.GetOrigin()
	e.Fields["deployment"] = msg.GetDeployment()
	e.Fields["ip"] = msg.GetIp()
	e.Fields["job"] = msg.GetJob()
	e.Fields["index"] = msg.GetIndex()
	//e.Fields["tags"] = msg.GetTags()
	e.Type = msg.GetEventType().String()

}

func HttpStart(msg *events.Envelope) Event {
	httpStart := msg.GetHttpStart()

	fields := logrus.Fields{
		"cf_app_id":         utils.FormatUUID(httpStart.GetApplicationId()),
		"instance_id":       httpStart.GetInstanceId(),
		"instance_index":    httpStart.GetInstanceIndex(),
		"method":            httpStart.GetMethod(),
		"parent_request_id": utils.FormatUUID(httpStart.GetParentRequestId()),
		"peer_type":         httpStart.GetPeerType(),
		"request_id":        utils.FormatUUID(httpStart.GetRequestId()),
		"remote_addr":       httpStart.GetRemoteAddress(),
		"timestamp":         httpStart.GetTimestamp(),
		"uri":               httpStart.GetUri(),
		"user_agent":        httpStart.GetUserAgent(),
	}

	return Event{
		Fields: fields,
		Msg:    "",
	}
}

func HttpStop(msg *events.Envelope) Event {
	httpStop := msg.GetHttpStop()

	fields := logrus.Fields{
		"cf_app_id":      utils.FormatUUID(httpStop.GetApplicationId()),
		"content_length": httpStop.GetContentLength(),
		"peer_type":      httpStop.GetPeerType(),
		"request_id":     utils.FormatUUID(httpStop.GetRequestId()),
		"status_code":    httpStop.GetStatusCode(),
		"timestamp":      httpStop.GetTimestamp(),
		"uri":            httpStop.GetUri(),
	}

	return Event{
		Fields: fields,
		Msg:    "",
	}
}

func HttpStartStop(msg *events.Envelope) Event {
	httpStartStop := msg.GetHttpStartStop()

	fields := logrus.Fields{
		"cf_app_id":       utils.FormatUUID(httpStartStop.GetApplicationId()),
		"content_length":  httpStartStop.GetContentLength(),
		"instance_id":     httpStartStop.GetInstanceId(),
		"instance_index":  httpStartStop.GetInstanceIndex(),
		"method":          httpStartStop.GetMethod(),
		"peer_type":       httpStartStop.GetPeerType(),
		"remote_addr":     httpStartStop.GetRemoteAddress(),
		"request_id":      utils.FormatUUID(httpStartStop.GetRequestId()),
		"start_timestamp": httpStartStop.GetStartTimestamp(),
		"status_code":     httpStartStop.GetStatusCode(),
		"stop_timestamp":  httpStartStop.GetStopTimestamp(),
		"uri":             httpStartStop.GetUri(),
		"user_agent":      httpStartStop.GetUserAgent(),
		"duration_ms":     (((httpStartStop.GetStopTimestamp() - httpStartStop.GetStartTimestamp()) / 1000) / 1000),
	}

	return Event{
		Fields: fields,
		Msg:    "",
	}
}

func LogMessage(msg *events.Envelope) Event {
	logMessage := msg.GetLogMessage()

	fields := logrus.Fields{
		"cf_app_id":       logMessage.GetAppId(),
		"timestamp":       logMessage.GetTimestamp(),
		"source_type":     logMessage.GetSourceType(),
		"message_type":    logMessage.GetMessageType().String(),
		"source_instance": logMessage.GetSourceInstance(),
	}

	return Event{
		Fields: fields,
		Msg:    string(logMessage.GetMessage()),
	}
}

func ValueMetric(msg *events.Envelope) Event {
	valMetric := msg.GetValueMetric()

	fields := logrus.Fields{
		"name":  valMetric.GetName(),
		"unit":  valMetric.GetUnit(),
		"value": valMetric.GetValue(),
	}

	return Event{
		Fields: fields,
		Msg:    "",
	}
}

func CounterEvent(msg *events.Envelope) Event {
	counterEvent := msg.GetCounterEvent()

	fields := logrus.Fields{
		"name":  counterEvent.GetName(),
		"delta": counterEvent.GetDelta(),
		"total": counterEvent.GetTotal(),
	}

	return Event{
		Fields: fields,
		Msg:    "",
	}
}

func ErrorEvent(msg *events.Envelope) Event {
	errorEvent := msg.GetError()

	fields := logrus.Fields{
		"code":  errorEvent.GetCode(),
		"delta": errorEvent.GetSource(),
	}

	return Event{
		Fields: fields,
		Msg:    errorEvent.GetMessage(),
	}
}

func ContainerMetric(msg *events.Envelope) Event {
	containerMetric := msg.GetContainerMetric()

	fields := logrus.Fields{
		"cf_app_id":      containerMetric.GetApplicationId(),
		"cpu_percentage": containerMetric.GetCpuPercentage(),
		"disk_bytes":     containerMetric.GetDiskBytes(),
		"instance_index": containerMetric.GetInstanceIndex(),
		"memory_bytes":   containerMetric.GetMemoryBytes(),
	}

	return Event{
		Fields: fields,
		Msg:    "",
	}
}

func (e *Event) AnnotateWithAppData() {

	cf_app_id := e.Fields["cf_app_id"]
	appGuid := fmt.Sprintf("%s", cf_app_id)

	if cf_app_id != nil && appGuid != "<nil>" && cf_app_id != "" {
		appInfo := getAppInfo(appGuid)
		cf_app_name := appInfo.Name
		cf_space_id := appInfo.SpaceGuid
		cf_space_name := appInfo.SpaceName
		cf_org_id := appInfo.OrgGuid
		cf_org_name := appInfo.OrgName

		if cf_app_name != "" {
			e.Fields["cf_app_name"] = cf_app_name
		}

		if cf_space_id != "" {
			e.Fields["cf_space_id"] = cf_space_id
		}

		if cf_space_name != "" {
			e.Fields["cf_space_name"] = cf_space_name
		}

		if cf_org_id != "" {
			e.Fields["cf_org_id"] = cf_org_id
		}

		if cf_org_name != "" {
			e.Fields["cf_org_name"] = cf_org_name
		}
	}
}

func (e *Event) AnnotateWithMetaData(extraFields map[string]string) {
	e.Fields["cf_origin"] = "firehose"
	e.Fields["event_type"] = e.Type
	for k, v := range extraFields {
		e.Fields[k] = v
	}
}

func (e Event) ShipEvent() {

	defer func() {
		if r := recover(); r != nil {
			log.LogError("Recovered in event.Log()", r)
		}
	}()

	if logMessageLimit > 0 && len(e.Msg) > logMessageLimit {
		count := 0
		index := 0
		length := len(e.Msg)
		for {
			e.Fields["msg_seq"] = count
			remainingChars := length - index
			if remainingChars <= 0 {
				break
			}

			if remainingChars <= logMessageLimit {
				logrus.WithFields(e.Fields).Info(e.Msg[index:length])
				break
			} else {
				logrus.WithFields(e.Fields).Info(e.Msg[index : index+logMessageLimit])
				index += logMessageLimit
				count++
			}
		}
	} else {
		logrus.WithFields(e.Fields).Info(e.Msg)
	}
}

func LogEventTotals(logTotalsTime time.Duration, dopplerEndpoint string) {
	firehoseEventTotals := time.NewTicker(logTotalsTime)
	count := uint64(0)
	startTime := time.Now()
	totalTime := startTime

	go func() {
		for range firehoseEventTotals.C {
			elapsedTime := time.Since(startTime).Seconds()
			totalElapsedTime := time.Since(totalTime).Seconds()
			startTime = time.Now()
			output, lastCount := getEventTotals(totalElapsedTime, elapsedTime, count, dopplerEndpoint)
			count = lastCount
			log.LogStd(output, true)
		}
	}()

	silentTicker := time.NewTicker(silentTime)
	silentLast := uint64(0)
	go func() {
		for range silentTicker.C {
			total := GetTotalCountOfSelectedEvents()
			if total-silentLast == 0 {
				panic("Did not get any events in the last " + silentTime.String())
			}
			silentLast = total
		}
	}()
}

func SetLogMessageLimit(limit int) {
	logMessageLimit = limit
}

func SetSilentTime(t time.Duration) {
	silentTime = t
}

func getEventTotals(totalElapsedTime float64, elapsedTime float64, lastCount uint64, dopplerEndpoint string) (string, uint64) {
	selectedEvents := GetSelectedEventsCount()
	totalCount := GetTotalCountOfSelectedEvents()
	sinceLastTime := float64(int(elapsedTime*10)) / 10
	sinceStartTime := float64(int(totalElapsedTime*10)) / 10

	var s string
	s = fmt.Sprintln(s,
		"\nWe have processed", totalCount-lastCount, "events from the firehose at",
		dopplerEndpoint, "over the last", sinceLastTime, "seconds and", totalCount, "total events since startup")
	s = fmt.Sprintln(s, "\nAnd of those events, we have processed\n")
	for event, count := range selectedEvents {
		s = fmt.Sprintln(s, "\n", count, event, "over the last", sinceStartTime, "seconds")
	}
	return s, totalCount
}
