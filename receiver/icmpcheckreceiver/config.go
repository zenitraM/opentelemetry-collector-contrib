// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package icmpcheckreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/icmpcheckreceiver"

import (
	"errors"
	"time"

	"go.opentelemetry.io/collector/scraper/scraperhelper"
	"go.uber.org/multierr"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/icmpcheckreceiver/internal/metadata"
)

var (
	errMissingTarget       = errors.New("must specify at least one target")
	errMissingTargetHost   = errors.New("target host is required")
	errInvalidTrafficClass = errors.New("traffic_class must be between 0 and 255")
)

type Config struct {
	scraperhelper.ControllerConfig `mapstructure:",squash"`
	metadata.MetricsBuilderConfig  `mapstructure:",squash"`
	Targets                        []PingTarget `mapstructure:"targets"`

	// prevent unkeyed literal initialization
	_ struct{}
}
type PingTarget struct {
	Host         string        `mapstructure:"host"`
	PingCount    int           `mapstructure:"ping_count,omitempty"`
	PingTimeout  time.Duration `mapstructure:"ping_timeout,omitempty"`
	PingInterval time.Duration `mapstructure:"ping_interval,omitempty"`
	TrafficClass *int          `mapstructure:"traffic_class,omitempty"`
}

func (c *Config) Validate() error {
	var err error

	if len(c.Targets) == 0 {
		return multierr.Append(err, errMissingTarget)
	}

	for _, target := range c.Targets {
		if target.Host == "" {
			err = multierr.Append(err, errMissingTargetHost)
		}
		if target.TrafficClass != nil {
			if *target.TrafficClass < 0 || *target.TrafficClass > 255 {
				err = multierr.Append(err, errInvalidTrafficClass)
			}
		}
	}

	return err
}
