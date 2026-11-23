package main

import (
	"flag"
	"github.com/seaweedfs/seaweed-block/core/hostlimit"
)

func registerHostLimitFlags(fs *flag.FlagSet, c *hostlimit.Config) {
	*c = hostlimit.DefaultConfig("")
	fs.StringVar(&c.Directory, "host-limit-dir", "", "shared local directory for background-cut slots; empty disables shared slots")
	fs.IntVar(&c.NormalSlots, "host-limit-normal-slots", c.NormalSlots, "host normal-cut slots; identical across processes")
	fs.IntVar(&c.UrgentSlots, "host-limit-urgent-slots", c.UrgentSlots, "host reserved urgent-cut slots")
	fs.IntVar(&c.FillSlots, "host-limit-fill-slots", c.FillSlots, "host background-fill slots; demand hydration bypasses them")
	fs.IntVar(&c.IdleSlots, "host-limit-idle-slots", c.IdleSlots, "host idle-work slots")
	fs.Int64Var(&c.PayloadBytes, "host-limit-payload-bytes", c.PayloadBytes, "maximum cut payload per slot")
	fs.Int64Var(&c.COWBytes, "host-limit-cow-bytes", c.COWBytes, "maximum saved COW bytes per slot")
	fs.Int64Var(&c.BytesPerSecond, "host-limit-upload-bytes-sec", c.BytesPerSecond, "pacing parameter divided per process by cut-slot count; not a host rate cap")
	fs.IntVar(&c.FallbackVolumes, "host-limit-fallback-volumes", c.FallbackVolumes, "kept for configuration compatibility; the urgent fallback is not paced (no shared host cap to protect)")
	fs.DurationVar(&c.MaxHold, "host-limit-max-hold", c.MaxHold, "cut lease timeout; expiration aborts but waits for admitted I/O before release")
}
