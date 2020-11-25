//+build integration

package monitor

import (
	"io/ioutil"
	"net/http"
	"regexp"
	"testing"

	"github.com/tal-tech/go-zero/core/prometheus"
)

func TestExample(t *testing.T) {
	// 设置一个计数器，并且计数器会是5次
	example()

	// 模拟prometheus从metrics拉取，计数器应该为5
	res, err := http.Get("http://localhost:8891/metrics")
	if err != nil {
		t.Error(err)
	}
	defer res.Body.Close()
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		t.Error(err)
	}
	//t.Log(string(b))
	reg, err := regexp.Compile("a_b_c{action=\"cc\",host=\".*\",status=\"bb\"} 5")
	if err != nil {
		t.Error(err)
	}
	result := reg.Find(b)
	if len(result) == 0 {
		t.Fatal()
	}
	t.Log(string(result))
}

func example() {
	prometheus.StartAgent(prometheus.Config{
		Host: "0.0.0.0",
		Port: 8891,
		Path: "/metrics",
	})
	vec := NewCountVec("a", "b", "c")
	label := &CountLabels{
		Status: "bb",
		Action: "cc",
	}
	for i := 0; i < 5; i++ {
		vec.Inc(label)
	}
}
