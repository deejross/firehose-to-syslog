package caching

import (
	"time"
)

type App struct {
	Name      string
	Guid      string
	SpaceName string
	SpaceGuid string
	OrgName   string
	OrgGuid   string
}

//go:generate counterfeiter . Caching

type Caching interface {
	CreateBucket()
	PerformPoollingCaching(time.Duration)
	GetAppByGuid(string) []App
	GetAllApp() []App
	GetAppInfo(string) App
	GetAppInfoCache(string) App
	CloseDB()
}
