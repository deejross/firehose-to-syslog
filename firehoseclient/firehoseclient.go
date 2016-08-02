package firehoseclient

import (
	"crypto/tls"
	"github.com/Pivotal-Japan/firehose-to-fluentd/eventRouting"
	log "github.com/Pivotal-Japan/firehose-to-fluentd/logging"
	"github.com/cloudfoundry/noaa/consumer"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gorilla/websocket"
	"time"
)

type FirehoseNozzle struct {
	errs             <-chan error
	messages         <-chan *events.Envelope
	authTokenFetcher AuthTokenFetcher
	consumer         *consumer.Consumer
	eventRouting     *eventRouting.EventRouting
	log              *log.Logging
	config           *FirehoseConfig
}

type FirehoseConfig struct {
	TrafficControllerURL   string
	InsecureSSLSkipVerify  bool
	IdleTimeoutSeconds     time.Duration
	FirehoseSubscriptionID string
}

type AuthTokenFetcher interface {
	GetToken() string
}

func NewFirehoseNozzle(tokenFetcher AuthTokenFetcher, eventRouting *eventRouting.EventRouting, firehoseconfig *FirehoseConfig, log *log.Logging) *FirehoseNozzle {
	return &FirehoseNozzle{
		authTokenFetcher: tokenFetcher,
		log:              log,
		errs:             make(<-chan error),
		messages:         make(<-chan *events.Envelope),
		eventRouting:     eventRouting,
		config:           firehoseconfig,
	}
}

func (f *FirehoseNozzle) Start() error {
	var authToken string
	authToken = f.authTokenFetcher.GetToken()
	f.consumeFirehose(authToken)
	err := f.routeEvent()
	return err
}

func (f *FirehoseNozzle) consumeFirehose(authToken string) {
	f.consumer = consumer.New(
		f.config.TrafficControllerURL,
		&tls.Config{InsecureSkipVerify: f.config.InsecureSSLSkipVerify},
		nil)
	f.consumer.SetIdleTimeout(time.Duration(f.config.IdleTimeoutSeconds) * time.Second)
	f.messages, f.errs = f.consumer.Firehose(f.config.FirehoseSubscriptionID, authToken)
}

func (f *FirehoseNozzle) routeEvent() error {
	for {
		select {
		case envelope := <-f.messages:
			f.eventRouting.RouteEvent(envelope)
		case err := <-f.errs:
			f.handleError(err)
			return err
		}
	}
}

func (f *FirehoseNozzle) handleError(err error) {
	switch closeErr := err.(type) {
	case *websocket.CloseError:
		switch closeErr.Code {
		case websocket.CloseNormalClosure:
		// no op
		case websocket.ClosePolicyViolation:
			f.log.LogError("Error while reading from the firehose: %v", err)
			f.log.LogError("Disconnected because nozzle couldn't keep up. Please try scaling up the nozzle.", nil)
		default:
			f.log.LogError("Error while reading from the firehose: %v", err)
		}
	default:
		f.log.LogError("Error while reading from the firehose: %v", err)

	}

	f.log.LogError("Closing connection with traffic controller due to %v", err)
	f.consumer.Close()
}

func (f *FirehoseNozzle) handleMessage(envelope *events.Envelope) {
	if envelope.GetEventType() == events.Envelope_CounterEvent && envelope.CounterEvent.GetName() == "TruncatingBuffer.DroppedMessages" && envelope.GetOrigin() == "doppler" {
		f.log.LogStd("We've intercepted an upstream message which indicates that the nozzle or the TrafficController is not keeping up. Please try scaling up the nozzle.", true)
	}
}
