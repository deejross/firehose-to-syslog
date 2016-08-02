package caching

import (
	"fmt"
	"github.com/Pivotal-Japan/firehose-to-fluentd/logging"
	"github.com/boltdb/bolt"
	cfClient "github.com/cloudfoundry-community/go-cfclient"
	json "github.com/pquerna/ffjson/ffjson"
	"log"
	"os"
	"time"
)

type CachingBolt struct {
	GcfClient *cfClient.Client
	Appdb     *bolt.DB
	log       *logging.Logging
}

func NewCachingBolt(gcfClientSet *cfClient.Client, logging *logging.Logging, boltDatabasePath string) Caching {

	//Use bolt for in-memory  - file caching
	db, err := bolt.Open(boltDatabasePath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal("Error opening bolt db: ", err)
		os.Exit(1)

	}

	return &CachingBolt{
		GcfClient: gcfClientSet,
		Appdb:     db,
		log:       logging,
	}
}

func (c *CachingBolt) CreateBucket() {
	c.Appdb.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("AppBucket"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil

	})

}

func (c *CachingBolt) PerformPoollingCaching(tickerTime time.Duration) {
	// Ticker Pooling the CC every X sec
	ccPooling := time.NewTicker(tickerTime)

	var apps []App
	go func() {
		for range ccPooling.C {
			apps = c.GetAllApp()
		}
	}()

}

func (c *CachingBolt) PerformStat() {
	go func() {
		// Grab the initial stats.
		prev := c.Appdb.Stats()

		for {
			time.Sleep(60 * time.Second)
			stats := c.Appdb.Stats()
			diff := stats.Sub(&prev)
			json.NewEncoder(os.Stdout).Encode(diff)
			prev = stats
		}
	}()
}

func (c *CachingBolt) FillDatabase(listApps []App) {
	for _, app := range listApps {
		c.Appdb.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("AppBucket"))
			if err != nil {
				return fmt.Errorf("create bucket: %s", err)
			}

			serialize, err := json.Marshal(app)

			if err != nil {
				return fmt.Errorf("Error Marshaling data: %s", err)
			}
			err = b.Put([]byte(app.Guid), serialize)

			if err != nil {
				return fmt.Errorf("Error inserting data: %s", err)
			}
			return nil
		})

	}

}

func (c *CachingBolt) GetAppByGuid(appGuid string) []App {
	var apps []App
	app := c.GcfClient.AppByGuid(appGuid)
	apps = append(apps, App{app.Name, app.Guid, app.SpaceData.Entity.Name, app.SpaceData.Entity.Guid, app.SpaceData.Entity.OrgData.Entity.Name, app.SpaceData.Entity.OrgData.Entity.Guid})
	c.FillDatabase(apps)
	return apps

}

func (c *CachingBolt) GetAllApp() []App {

	c.log.LogStd("Retrieving Apps for Cache...", false)
	var apps []App

	defer func() {
		if r := recover(); r != nil {
			c.log.LogError("Recovered in caching.GetAllApp()", r)
		}
	}()

	for _, app := range c.GcfClient.ListApps() {
		c.log.LogStd(fmt.Sprintf("App [%s] Found...", app.Name), false)
		apps = append(apps, App{app.Name, app.Guid, app.SpaceData.Entity.Name, app.SpaceData.Entity.Guid, app.SpaceData.Entity.OrgData.Entity.Name, app.SpaceData.Entity.OrgData.Entity.Guid})
	}

	c.FillDatabase(apps)
	c.log.LogStd(fmt.Sprintf("Found [%d] Apps!", len(apps)), false)

	return apps
}

func (c *CachingBolt) GetAppInfo(appGuid string) App {

	var d []byte
	var app App
	c.Appdb.View(func(tx *bolt.Tx) error {
		c.log.LogStd(fmt.Sprintf("Looking for App %s in Cache!\n", appGuid), false)
		b := tx.Bucket([]byte("AppBucket"))
		d = b.Get([]byte(appGuid))
		return nil
	})
	err := json.Unmarshal([]byte(d), &app)
	if err != nil {
		return App{}
	}
	return app
}

func (c *CachingBolt) CloseDB() {
	c.Appdb.Close()
}

func (c *CachingBolt) GetAppInfoCache(appGuid string) App {
	if app := c.GetAppInfo(appGuid); app.Name != "" {
		return app
	} else {
		c.GetAppByGuid(appGuid)
	}
	return c.GetAppInfo(appGuid)
}
