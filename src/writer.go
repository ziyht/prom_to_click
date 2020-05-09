package src

import "time"

type promSample struct {
	name string
	tags []string
	val  float64
	ts   time.Time
}

func initWriter(){

}
