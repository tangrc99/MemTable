package global

import "time"

const (
	MaxTTLEvict = 100
)

/* ---------------------------------------------------------------------------
* TimeEvents
* ------------------------------------------------------------------------- */

const (
	TECleanClients = 10 * time.Second
	TEExpireKey    = time.Second
	TEAOF          = time.Second
	TEBgSave       = 5 * time.Second
	TEUpdateStatus = time.Second
	TEReplica      = 200 * time.Millisecond
	TECluster      = 200 * time.Millisecond
)

const (
	RsMaxIdle    = 10
	RsBackLogCap = 1 << 20
	RsMaxSendLen = 1 << 10
)
