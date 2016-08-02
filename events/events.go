package events

import (
	"fmt"
	"github.com/Pivotal-Japan/firehose-to-fluentd/caching"
	"github.com/Pivotal-Japan/firehose-to-fluentd/utils"
	"github.com/Sirupsen/logrus"
	"github.com/cloudfoundry/sonde-go/events"
)

type Event struct {
	Fields logrus.Fields
	Msg    string
	Type   string
}

func HttpStart(msg *events.Envelope) *Event {
	httpStart := msg.GetHttpStart()

	fields := logrus.Fields{
		"cf_app_id":         utils.FormatUUID(httpStart.GetApplicationId()),
		"instance_id":       httpStart.GetInstanceId(),
		"instance_index":    httpStart.GetInstanceIndex(),
		"method":            httpStart.GetMethod().String(),
		"parent_request_id": utils.FormatUUID(httpStart.GetParentRequestId()),
		"peer_type":         httpStart.GetPeerType().String(),
		"request_id":        utils.FormatUUID(httpStart.GetRequestId()),
		"remote_addr":       httpStart.GetRemoteAddress(),
		"timestamp":         httpStart.GetTimestamp(),
		"uri":               httpStart.GetUri(),
		"user_agent":        httpStart.GetUserAgent(),
		"tag":               "cf." + msg.GetEventType().String(),
	}

	return &Event{
		Fields: fields,
		Msg:    "",
	}
}

func HttpStop(msg *events.Envelope) *Event {
	httpStop := msg.GetHttpStop()

	fields := logrus.Fields{
		"cf_app_id":      utils.FormatUUID(httpStop.GetApplicationId()),
		"content_length": httpStop.GetContentLength(),
		"peer_type":      httpStop.GetPeerType().String(),
		"request_id":     utils.FormatUUID(httpStop.GetRequestId()),
		"status_code":    httpStop.GetStatusCode(),
		"timestamp":      httpStop.GetTimestamp(),
		"uri":            httpStop.GetUri(),
		"tag":            "cf." + msg.GetEventType().String(),
	}

	return &Event{
		Fields: fields,
		Msg:    "",
	}
}

func HttpStartStop(msg *events.Envelope) *Event {
	httpStartStop := msg.GetHttpStartStop()

	fields := logrus.Fields{
		"cf_app_id":       utils.FormatUUID(httpStartStop.GetApplicationId()),
		"content_length":  httpStartStop.GetContentLength(),
		"instance_id":     httpStartStop.GetInstanceId(),
		"instance_index":  httpStartStop.GetInstanceIndex(),
		"method":          httpStartStop.GetMethod().String(),
		"peer_type":       httpStartStop.GetPeerType().String(),
		"remote_addr":     httpStartStop.GetRemoteAddress(),
		"request_id":      utils.FormatUUID(httpStartStop.GetRequestId()),
		"start_timestamp": httpStartStop.GetStartTimestamp(),
		"status_code":     httpStartStop.GetStatusCode(),
		"stop_timestamp":  httpStartStop.GetStopTimestamp(),
		"uri":             httpStartStop.GetUri(),
		"user_agent":      httpStartStop.GetUserAgent(),
		"duration_ms":     (((httpStartStop.GetStopTimestamp() - httpStartStop.GetStartTimestamp()) / 1000) / 1000),
		"tag":             "cf." + msg.GetEventType().String(),
	}

	return &Event{
		Fields: fields,
		Msg:    "",
	}
}

func LogMessage(msg *events.Envelope) *Event {
	logMessage := msg.GetLogMessage()

	fields := logrus.Fields{
		"cf_app_id":       logMessage.GetAppId(),
		"timestamp":       logMessage.GetTimestamp(),
		"source_type":     logMessage.GetSourceType(),
		"message_type":    logMessage.GetMessageType().String(),
		"source_instance": logMessage.GetSourceInstance(),
		"tag":             utils.ConcatFormat([]string{"cf", "LogMessage", logMessage.GetSourceType(), logMessage.GetMessageType().String()}),
	}

	return &Event{
		Fields: fields,
		Msg:    string(logMessage.GetMessage()),
	}
}

func ValueMetric(msg *events.Envelope) *Event {
	valMetric := msg.GetValueMetric()

	fields := logrus.Fields{
		"name":  valMetric.GetName(),
		"unit":  valMetric.GetUnit(),
		"value": valMetric.GetValue(),
		"tag":   "cf." + msg.GetEventType().String(),
	}

	return &Event{
		Fields: fields,
		Msg:    "",
	}
}

func CounterEvent(msg *events.Envelope) *Event {
	counterEvent := msg.GetCounterEvent()

	fields := logrus.Fields{
		"name":  counterEvent.GetName(),
		"delta": counterEvent.GetDelta(),
		"total": counterEvent.GetTotal(),
		"tag":   "cf." + msg.GetEventType().String(),
	}

	return &Event{
		Fields: fields,
		Msg:    "",
	}
}

func ErrorEvent(msg *events.Envelope) *Event {
	errorEvent := msg.GetError()

	fields := logrus.Fields{
		"code":  errorEvent.GetCode(),
		"delta": errorEvent.GetSource(),
		"tag":   "cf." + msg.GetEventType().String(),
	}

	return &Event{
		Fields: fields,
		Msg:    errorEvent.GetMessage(),
	}
}

func ContainerMetric(msg *events.Envelope) *Event {
	containerMetric := msg.GetContainerMetric()

	fields := logrus.Fields{
		"cf_app_id":      containerMetric.GetApplicationId(),
		"cpu_percentage": containerMetric.GetCpuPercentage(),
		"disk_bytes":     containerMetric.GetDiskBytes(),
		"instance_index": containerMetric.GetInstanceIndex(),
		"memory_bytes":   containerMetric.GetMemoryBytes(),
		"tag":            "cf." + msg.GetEventType().String(),
	}

	return &Event{
		Fields: fields,
		Msg:    "",
	}
}

func (e *Event) AnnotateWithAppData(caching caching.Caching) {
	cf_app_id := e.Fields["cf_app_id"]
	appGuid := ""
	cluster := e.Fields["cluster"].(string)
	domain := e.Fields["domain"].(string)

	if cf_app_id != nil {
		appGuid = fmt.Sprintf("%s", cf_app_id)
	}

	if cf_app_id != nil && appGuid != "<nil>" && cf_app_id != "" {
		appInfo := caching.GetAppInfoCache(appGuid)
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

func (e *Event) AnnotateWithTag() {
	if e.Fields["cf_origin"] != nil && e.Fields["event_type"] != nil {
		e.Fields["tag"] = fmt.Sprintf("%s.%s", e.Fields["cf_origin"], e.Fields["event_type"])

	}
}

func (e *Event) AnnotateWithMetaData(extraFields map[string]string) {
	e.Fields["cf_origin"] = "firehose"
	e.Fields["event_type"] = e.Type
	for k, v := range extraFields {
		e.Fields[k] = v
	}
}

func (e *Event) AnnotateWithEnveloppeData(msg *events.Envelope) {
	e.Fields["origin"] = msg.GetOrigin()
	e.Fields["deployment"] = msg.GetDeployment()
	e.Fields["ip"] = msg.GetIp()
	e.Fields["job"] = msg.GetJob()
	e.Fields["index"] = msg.GetIndex()
	e.Type = msg.GetEventType().String()

}
