package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/boltdb/bolt"
	"github.com/cloudfoundry-community/go-cfclient"
	"github.com/deejross/firehose-to-syslog/caching"
	"github.com/deejross/firehose-to-syslog/events"
	"github.com/deejross/firehose-to-syslog/extrafields"
	"github.com/deejross/firehose-to-syslog/firehose"
	"github.com/deejross/firehose-to-syslog/logging"
	"github.com/pkg/profile"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	debug              = kingpin.Flag("debug", "Enable debug mode. This disables forwarding to syslog").Default("false").OverrideDefaultFromEnvar("DEBUG").Bool()
	apiEndpoint        = kingpin.Flag("api-endpoint", "Api endpoint address. For bosh-lite installation of CF: https://api.10.244.0.34.xip.io").OverrideDefaultFromEnvar("API_ENDPOINT").Required().String()
	dopplerEndpoint    = kingpin.Flag("doppler-endpoint", "Overwrite default doppler endpoint return by /v2/info").OverrideDefaultFromEnvar("DOPPLER_ENDPOINT").String()
	syslogServer       = kingpin.Flag("syslog-server", "Syslog server.").OverrideDefaultFromEnvar("SYSLOG_ENDPOINT").String()
	syslogProtocol     = kingpin.Flag("syslog-protocol", "Syslog protocol (tcp/udp).").Default("tcp").OverrideDefaultFromEnvar("SYSLOG_PROTOCOL").String()
	subscriptionId     = kingpin.Flag("subscription-id", "Id for the subscription.").Default("firehose").OverrideDefaultFromEnvar("FIREHOSE_SUBSCRIPTION_ID").String()
	user               = kingpin.Flag("user", "Admin user.").Default("admin").OverrideDefaultFromEnvar("FIREHOSE_USER").String()
	password           = kingpin.Flag("password", "Admin password.").Default("admin").OverrideDefaultFromEnvar("FIREHOSE_PASSWORD").String()
	skipSSLValidation  = kingpin.Flag("skip-ssl-validation", "Please don't").Default("false").OverrideDefaultFromEnvar("SKIP_SSL_VALIDATION").Bool()
	keepAlive          = kingpin.Flag("fh-keep-alive", "Keep Alive duration for the firehose consumer").Default("25s").OverrideDefaultFromEnvar("FH_KEEP_ALIVE").Duration()
	logEventTotals     = kingpin.Flag("log-event-totals", "Logs the counters for all selected events since nozzle was last started.").Default("false").OverrideDefaultFromEnvar("LOG_EVENT_TOTALS").Bool()
	logEventTotalsTime = kingpin.Flag("log-event-totals-time", "How frequently the event totals are calculated (in sec).").Default("30s").OverrideDefaultFromEnvar("LOG_EVENT_TOTALS_TIME").Duration()
	wantedEvents       = kingpin.Flag("events", fmt.Sprintf("Comma separated list of events you would like. Valid options are %s", events.GetListAuthorizedEventEvents())).Default("LogMessage").OverrideDefaultFromEnvar("EVENTS").String()
	boltDatabasePath   = kingpin.Flag("boltdb-path", "Bolt Database path ").Default("my.db").OverrideDefaultFromEnvar("BOLTDB_PATH").String()
	tickerTime         = kingpin.Flag("cc-pull-time", "CloudController Polling time in sec").Default("60s").OverrideDefaultFromEnvar("CF_PULL_TIME").Duration()
	extraFields        = kingpin.Flag("extra-fields", "Extra fields you want to annotate your events with, example: '--extra-fields=env:dev,something:other ").Default("").OverrideDefaultFromEnvar("EXTRA_FIELDS").String()
	modeProf           = kingpin.Flag("mode-prof", "Enable profiling mode, one of [cpu, mem, block]").Default("").OverrideDefaultFromEnvar("MODE_PROF").String()
	pathProf           = kingpin.Flag("path-prof", "Set the Path to write profiling file").Default("").OverrideDefaultFromEnvar("PATH_PROF").String()
	logFormatterType   = kingpin.Flag("log-formatter-type", "Log formatter type to use. Valid options are text, json. If none provided, defaults to json.").Envar("LOG_FORMATTER_TYPE").String()
	logMessageLimit    = kingpin.Flag("log-message-limit", "Split log messages longer than a given limit").Default("0").OverrideDefaultFromEnvar("LOG_MESSAGE_LIMIT").Int()
	silentTime         = kingpin.Flag("silent-time", "Time without a log message before crashing. This is to address the stale firehose connection issue, default 5 minutes").Default("5m").OverrideDefaultFromEnvar("SILENT_TIME").Duration()
)

var (
	version = "0.0.0"
)

func main() {
	kingpin.Version(version)
	kingpin.Parse()
	logging.LogStd(fmt.Sprintf("Starting firehose-to-syslog %s ", version), true)

	logging.SetupLogging(*syslogServer, *syslogProtocol, *debug, *logFormatterType)

	c := cfclient.Config{
		ApiAddress:        *apiEndpoint,
		Username:          *user,
		Password:          *password,
		SkipSslValidation: *skipSSLValidation,
	}
	cfClient := cfclient.NewClient(&c)

	if len(*dopplerEndpoint) > 0 {
		cfClient.Endpoint.DopplerEndpoint = *dopplerEndpoint
	}
	logging.LogStd(fmt.Sprintf("Using %s as doppler endpoint", cfClient.Endpoint.DopplerEndpoint), true)

	logging.LogStd("Setting up event routing!", true)
	err := events.SetupEventRouting(*wantedEvents)
	if err != nil {
		log.Fatal("Error setting up event routing: ", err)
		os.Exit(1)

	}

	if *modeProf != "" {
		switch *modeProf {
		case "cpu":
			defer profile.Start(profile.CPUProfile, profile.ProfilePath(*pathProf)).Stop()
		case "mem":
			defer profile.Start(profile.MemProfile, profile.ProfilePath(*pathProf)).Stop()
		case "block":
			defer profile.Start(profile.BlockProfile, profile.ProfilePath(*pathProf)).Stop()
		default:
			// do nothing
		}
	}

	if caching.IsNeeded(*wantedEvents) {
		//Use bolt for in-memory  - file caching
		db, err := bolt.Open(*boltDatabasePath, 0600, &bolt.Options{Timeout: 1 * time.Second})
		if err != nil {
			log.Fatal("Error opening bolt db: ", err)
			os.Exit(1)
		}
		defer db.Close()

		caching.SetCfClient(cfClient)
		caching.SetAppDb(db)
		caching.CreateBucket()

		//Let's Update the database the first time
		logging.LogStd("Start filling app/space/org cache.", true)
		apps := caching.GetAllApp()
		logging.LogStd(fmt.Sprintf("Done filling cache! Found [%d] Apps", len(apps)), true)

		// Ticker Pooling the CC every X sec
		ccPooling := time.NewTicker(*tickerTime)

		go func() {
			for range ccPooling.C {
				apps = caching.GetAllApp()
			}
		}()
	}

	// Parse extra fields from cmd call
	extraFields, err := extrafields.ParseExtraFields(*extraFields)
	if err != nil {
		log.Fatal("Error parsing extra fields: ", err)
		os.Exit(1)
	}

	if *logEventTotals == true {
		events.SetSilentTime(*silentTime)
		events.LogEventTotals(*logEventTotalsTime, *dopplerEndpoint)
	}

	// set max message length per message
	events.SetLogMessageLimit(*logMessageLimit)	

	if logging.Connect() || *debug {

		logging.LogStd("Connected to Syslog Server! Connecting to Firehose...", true)

		firehose := firehose.CreateFirehoseChan(cfClient.Endpoint.DopplerEndpoint, cfClient.GetToken(), *subscriptionId, *skipSSLValidation, *keepAlive)
		if firehose != nil {
			logging.LogStd("Firehose Subscription Succesfull! Routing events...", true)
			events.RouteEvents(firehose, extraFields)
		} else {
			logging.LogError("Failed connecting to Firehose...Please check settings and try again!", "")
		}

	} else {
		logging.LogError("Failed connecting to the Syslog Server...Please check settings and try again!", "")
	}
}
