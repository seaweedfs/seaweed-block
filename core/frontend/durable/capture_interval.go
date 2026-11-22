package durable

import "time"

// MinCaptureInterval is the shortest automatic cut interval blockvolume
// accepts (--chunk-capture-interval; 0 turns the timer off). A program
// that builds blockvolume with a shorter floor sets it before parsing
// flags; nothing changes it at run time.
var MinCaptureInterval = 5 * time.Minute
