package service

import (
	"context"
	"testing"

	"github.com/olivere/elastic/v7"
	"github.com/quanxiang-cloud/cabin/logger"
	ec "github.com/quanxiang-cloud/cabin/tailormade/db/elastic"
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

	ctx := context.Background()
	_, err := bonding.InsertEdge(ctx, &InsertEdgeReq{
		Start: node{
			EndPoint: "A",
		},
		End: node{
			EndPoint: "B",
		},
		Lables: map[string]string{
			"test": "test",
		},
	})

	_, err = bonding.InsertEdge(ctx, &InsertEdgeReq{
		Start: node{
			EndPoint: "A",
		},
		End: node{
			EndPoint: "C",
		},
	})

	_, err = bonding.InsertEdge(ctx, &InsertEdgeReq{
		Start: node{
			EndPoint: "B",
		},
		End: node{
			EndPoint: "C",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = bonding.DeleteEdge(ctx, &DeleteEdgeReq{
		Start: node{
			EndPoint: "A",
		},
		Lables: map[string]string{
			"test": "test",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = bonding.DeleteEdge(ctx, &DeleteEdgeReq{
		Start: node{
			EndPoint: "B",
		},
		End: node{
			EndPoint: "C",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = bonding.DeleteEdge(ctx, &DeleteEdgeReq{
		Start: node{
			EndPoint: "A",
		},
		End: node{
			EndPoint: "C",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}
