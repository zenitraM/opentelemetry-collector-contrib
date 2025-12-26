// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package icmpcheckreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/icmpcheckreceiver"

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/icmpcheckreceiver/internal/metadata"
)

const (
	DefaultPingCount    = 3
	DefaultPingTimeout  = time.Second * 5
	DefaultPingInterval = time.Second * 1
)

type pingResult struct {
	stats        *pingStats
	targetHost   string
	targetIP     string
	trafficClass *int
	err          error
}

type icmpCheckScraper struct {
	cfg           *Config
	settings      component.TelemetrySettings
	mb            *metadata.MetricsBuilder
	pingerFactory func(target PingTarget) (pinger, error)
}

// apply defaults on targets if not set
func (scr *icmpCheckScraper) start(_ context.Context, _ component.Host) (err error) {
	if scr.pingerFactory == nil {
		scr.pingerFactory = defaultPingerFactory
	}
	for i := range scr.cfg.Targets {
		if scr.cfg.Targets[i].PingCount == 0 {
			scr.cfg.Targets[i].PingCount = DefaultPingCount
		}
		if scr.cfg.Targets[i].PingTimeout == 0 {
			scr.cfg.Targets[i].PingTimeout = DefaultPingTimeout
		}
		if scr.cfg.Targets[i].PingInterval == 0 {
			scr.cfg.Targets[i].PingInterval = DefaultPingInterval
		}
	}
	return nil
}

func (scr *icmpCheckScraper) scrape(_ context.Context) (pmetric.Metrics, error) {
	results := make(chan pingResult, len(scr.cfg.Targets))

	for _, target := range scr.cfg.Targets {
		p, err := scr.pingerFactory(target)
		if err != nil {
			results <- pingResult{
				targetHost:   target.Host,
				trafficClass: target.TrafficClass,
				err:          err,
			}
			continue
		}
		go ping(p, target.TrafficClass, results)
	}

	// Collect results and build metrics
	var pingResults []pingResult
	for range scr.cfg.Targets {
		result := <-results
		addMetrics(result, scr.mb, scr.settings.Logger)
		pingResults = append(pingResults, result)
	}

	metrics := scr.mb.Emit()

	// Add histogram metrics manually
	for _, result := range pingResults {
		addHistogramMetrics(metrics, result)
	}

	return metrics, nil
}

func ping(p pinger, trafficClass *int, results chan<- pingResult) {
	err := p.Run()
	if err != nil {
		results <- pingResult{
			targetHost:   p.HostName(),
			trafficClass: trafficClass,
			err:          err,
		}
		return
	}

	results <- pingResult{
		targetHost:   p.HostName(),
		targetIP:     p.IPString(),
		stats:        p.Stats(),
		trafficClass: trafficClass,
		err:          nil,
	}
}

func addMetrics(result pingResult, mb *metadata.MetricsBuilder, logger *zap.Logger) {
	now := pcommon.NewTimestampFromTime(time.Now())

	if result.err != nil {
		logger.Error(
			"failed to ping host",
			zap.String("host", result.targetHost),
			zap.Error(result.err))
		return
	}

	// Record existing gauge metrics
	mb.RecordPingRttMinDataPoint(now, result.stats.minRtt.Milliseconds())
	mb.RecordPingRttMaxDataPoint(now, result.stats.maxRtt.Milliseconds())
	mb.RecordPingRttAvgDataPoint(now, result.stats.avgRtt.Milliseconds())
	mb.RecordPingRttStddevDataPoint(now, result.stats.stdDevRtt.Milliseconds())
	mb.RecordPingLossRatioDataPoint(now, result.stats.lossRatio)

	// Record resource attributes
	rb := mb.NewResourceBuilder()
	rb.SetNetPeerName(result.targetHost)
	rb.SetNetPeerIP(result.targetIP)
	if result.trafficClass != nil {
		rb.SetNetTrafficClass(int64(*result.trafficClass))
	}

	mb.EmitForResource(metadata.WithResource(rb.Emit()))
}

// addHistogramMetrics creates histogram metrics for RTT distribution and packet loss
func addHistogramMetrics(metrics pmetric.Metrics, result pingResult) {
	if result.err != nil {
		return
	}

	now := pcommon.NewTimestampFromTime(time.Now())
	rm := metrics.ResourceMetrics().AppendEmpty()
	resource := rm.Resource()

	// Set resource attributes
	resource.Attributes().PutStr("net.peer.name", result.targetHost)
	resource.Attributes().PutStr("net.peer.ip", result.targetIP)
	if result.trafficClass != nil {
		resource.Attributes().PutInt("net.traffic.class", int64(*result.trafficClass))
	}

	sm := rm.ScopeMetrics().AppendEmpty()

	// Create RTT histogram metric
	if len(result.stats.individualRtts) > 0 {
		rttMetric := sm.Metrics().AppendEmpty()
		rttMetric.SetName("ping.rtt")
		rttMetric.SetDescription("Distribution of individual round-trip times.")
		rttMetric.SetUnit("ms")

		hist := rttMetric.SetEmptyHistogram()
		hist.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

		dp := hist.DataPoints().AppendEmpty()
		dp.SetTimestamp(now)
		dp.SetStartTimestamp(now)

		// Define bucket boundaries (in milliseconds)
		boundaries := []float64{0, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000}
		dp.ExplicitBounds().FromRaw(boundaries)

		// Count observations in each bucket
		bucketCounts := make([]uint64, len(boundaries)+1)
		var sum float64
		var count uint64

		for _, rtt := range result.stats.individualRtts {
			rttMs := float64(rtt.Milliseconds())
			sum += rttMs
			count++

			// Find which bucket this observation falls into
			bucketIdx := len(boundaries) // default to overflow bucket
			for i, bound := range boundaries {
				if rttMs <= bound {
					bucketIdx = i
					break
				}
			}
			bucketCounts[bucketIdx]++
		}

		dp.BucketCounts().FromRaw(bucketCounts)
		dp.SetCount(count)
		dp.SetSum(sum)
		if count > 0 {
			dp.SetMin(float64(result.stats.minRtt.Milliseconds()))
			dp.SetMax(float64(result.stats.maxRtt.Milliseconds()))
		}
	}

	// Create packet loss histogram metric
	lossMetric := sm.Metrics().AppendEmpty()
	lossMetric.SetName("ping.loss")
	lossMetric.SetDescription("Distribution of packet loss ratios.")
	lossMetric.SetUnit("1")

	lossHist := lossMetric.SetEmptyHistogram()
	lossHist.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	lossDp := lossHist.DataPoints().AppendEmpty()
	lossDp.SetTimestamp(now)
	lossDp.SetStartTimestamp(now)

	// Define bucket boundaries for loss ratio (0.0 to 1.0)
	lossBoundaries := []float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0}
	lossDp.ExplicitBounds().FromRaw(lossBoundaries)

	// Single observation for this scrape interval
	lossRatio := result.stats.lossRatio / 100.0
	lossBucketCounts := make([]uint64, len(lossBoundaries)+1)

	// Find which bucket this loss ratio falls into
	bucketIdx := len(lossBoundaries) // default to overflow bucket
	for i, bound := range lossBoundaries {
		if lossRatio <= bound {
			bucketIdx = i
			break
		}
	}
	lossBucketCounts[bucketIdx] = 1

	lossDp.BucketCounts().FromRaw(lossBucketCounts)
	lossDp.SetCount(1)
	lossDp.SetSum(lossRatio)
	lossDp.SetMin(lossRatio)
	lossDp.SetMax(lossRatio)
}

func newScraper(cfg *Config, settings receiver.Settings) *icmpCheckScraper {
	return &icmpCheckScraper{
		cfg:      cfg,
		settings: settings.TelemetrySettings,
		mb:       metadata.NewMetricsBuilder(cfg.MetricsBuilderConfig, settings),
	}
}
