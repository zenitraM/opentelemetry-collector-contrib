// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package icmpcheckreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/icmpcheckreceiver"

import (
	"sync"
	"time"

	probing "github.com/prometheus-community/pro-bing"
)

type pingStats struct {
	minRtt         time.Duration
	avgRtt         time.Duration
	maxRtt         time.Duration
	stdDevRtt      time.Duration
	lossRatio      float64
	individualRtts []time.Duration
}

type pinger = interface {
	Run() error
	Stats() *pingStats
	IPString() string
	HostName() string
}

type defaultPinger struct {
	*probing.Pinger
	rttValues []time.Duration
	mu        sync.Mutex
}

func (p *defaultPinger) IPString() string {
	return p.IPAddr().IP.String()
}

func (p *defaultPinger) HostName() string {
	return p.Addr()
}

func (p *defaultPinger) Stats() *pingStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	return &pingStats{
		minRtt:         p.Statistics().MinRtt,
		avgRtt:         p.Statistics().AvgRtt,
		maxRtt:         p.Statistics().MaxRtt,
		stdDevRtt:      p.Statistics().StdDevRtt,
		lossRatio:      p.Statistics().PacketLoss,
		individualRtts: append([]time.Duration{}, p.rttValues...),
	}
}

func defaultPingerFactory(target PingTarget) (pinger, error) {
	p, err := probing.NewPinger(target.Host)
	if err != nil {
		return nil, err
	}

	p.Interval = target.PingInterval
	p.Timeout = target.PingTimeout
	p.Count = target.PingCount

	dp := &defaultPinger{
		Pinger:    p,
		rttValues: make([]time.Duration, 0, target.PingCount),
	}

	// Capture individual RTT values
	p.OnRecv = func(pkt *probing.Packet) {
		dp.mu.Lock()
		dp.rttValues = append(dp.rttValues, pkt.Rtt)
		dp.mu.Unlock()
	}

	// Apply ToS/traffic class if configured
	if target.TrafficClass != nil {
		p.SetTrafficClass(uint8(*target.TrafficClass))
	}

	return dp, nil
}
