package service

import (
	"context"
	"testing"

	"github.com/olivere/elastic/v7"
	"github.com/quanxiang-cloud/cabin/logger"
	ec "github.com/quanxiang-cloud/cabin/tailormade/db/elastic"
	"github.com/quanxiang-cloud/linkbonding/pkg/apis/v1alpha1"
)

func newes(t *testing.T) *elastic.Client {
	esClient, err := ec.NewClient(&ec.Config{
		Host: []string{
			"http://es:9200",
		},
	}, logger.NewDefault())
	if err != nil {
		t.Fatal(err)
	}
	return esClient
}

func newBondonding(t *testing.T) *Bonding {
	b, err := NewBonding(WithElasticsearch(newes(t)))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestNode(t *testing.T) {
	bonding := newBondonding(t)

	var tests = []struct {
		start string
		end   string
		uid   string
	}{
		{
			start: "A",
			end:   "B",
			uid:   "1",
		},
		{
			start: "A",
			end:   "C",
			uid:   "1",
		},
		{
			start: "B",
			end:   "C",
			uid:   "1",
		},
		{
			start: "A",
			end:   "B",
			uid:   "2",
		},
		{
			start: "A",
			end:   "C",
			uid:   "2",
		},
		{
			start: "C",
			end:   "B",
			uid:   "2",
		},
	}

	ctx := context.Background()

	for _, test := range tests {
		_, err := bonding.InsertEdge(ctx, &InsertEdgeReq{
			Start: node{
				EndPoint: v1alpha1.EndPoint(test.start),
			},
			End: node{
				EndPoint: v1alpha1.EndPoint(test.end),
			},
			UID: test.uid,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, test := range tests {
		_, err := bonding.DeleteEdge(ctx, &DeleteEdgeReq{
			Start: node{
				EndPoint: v1alpha1.EndPoint(test.start),
			},
			End: node{
				EndPoint: v1alpha1.EndPoint(test.end),
			},
			UID: test.uid,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

}
