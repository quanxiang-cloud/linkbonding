package service

import (
	"context"

	"github.com/olivere/elastic/v7"
	"github.com/quanxiang-cloud/linkbonding/internal/models"
	"github.com/quanxiang-cloud/linkbonding/internal/models/elasticsearch"
	"github.com/quanxiang-cloud/linkbonding/pkg/apis/v1alpha1"
)

type Bonding struct {
	esClient *elastic.Client

	nodeRepo models.NodeRepo
}

// NewBonding return bonding service
func NewBonding(opts ...bonddingOption) (*Bonding, error) {
	b := &Bonding{}
	for _, opt := range opts {
		opt(b)
	}

	if err := b.init(); err != nil {
		return nil, err
	}

	return b, nil
}

func (b *Bonding) init() error {
	b.nodeRepo = elasticsearch.NewNode(b.esClient)

	return nil
}

type bonddingOption func(*Bonding)

// WithElasticsearch with elasticsearch client
func WithElasticsearch(c *elastic.Client) bonddingOption {
	return func(b *Bonding) {
		b.esClient = c
	}
}

type node struct {
	EndPoint v1alpha1.EndPoint `json:"endPoint"`
}

// InsertEdgeReq insert edge req
type InsertEdgeReq struct {
	Start  node              `json:"start,omitempty"`
	End    node              `json:"end,omitempty"`
	Lables map[string]string `json:"labels,omitempty"`
}

// InsertEdgeResp insert edge resp
type InsertEdgeResp struct {
}

// TODO should break cycle?
// TODO nodes are critical resources and need to use distributed lock control.
// TODO reference counter
// InsertEdge add an edge to a graph or generate a graph with only two vertices
func (b *Bonding) InsertEdge(ctx context.Context, req *InsertEdgeReq) (*InsertEdgeResp, error) {
	start, err := b.getNode(ctx, req.Start.EndPoint)
	if err != nil {
		return &InsertEdgeResp{}, err
	}
	end, err := b.getNode(ctx, req.End.EndPoint)
	if err != nil {
		return &InsertEdgeResp{}, err
	}

	if start.GetNext(req.End.EndPoint) == nil {
		if len(start.Nexts) == 0 {
			start.Nexts = make([]*v1alpha1.Edge, 0)
		}
		start.Nexts = append(start.Nexts, &v1alpha1.Edge{
			EndPoint: req.End.EndPoint,
			Labels:   req.Lables,
		})

		if len(end.Pres) == 0 {
			end.Pres = make([]*v1alpha1.Edge, 0)
		}
		end.Pres = append(end.Pres, &v1alpha1.Edge{
			EndPoint: req.Start.EndPoint,
		})
	}

	if err := b.nodeRepo.Insert(ctx, start); err != nil {
		return &InsertEdgeResp{}, err
	}
	if err := b.nodeRepo.Insert(ctx, end); err != nil {
		return &InsertEdgeResp{}, err
	}

	return &InsertEdgeResp{}, nil
}

// DeleteEdgeReq delete edge req
type DeleteEdgeReq struct {
	Start  node              `json:"start,omitempty"`
	End    node              `json:"end,omitempty"`
	Lables map[string]string `json:"labels,omitempty"`
}

// DeleteEdgeResp delete edge resp
type DeleteEdgeResp struct {
}

// DeleteEdge If the associated node has no extra edge,
// delete the node itself, otherwise delete the edge.
func (b *Bonding) DeleteEdge(ctx context.Context, req *DeleteEdgeReq) (*DeleteEdgeResp, error) {
	start, err := b.nodeRepo.GetNode(ctx, req.Start.EndPoint)
	if err != nil {
		return &DeleteEdgeResp{}, err
	}

	if start == nil {
		return &DeleteEdgeResp{}, err
	}

	ends := make([]*v1alpha1.Node, 0)
	switch {
	case req.End.EndPoint != "":
		end, err := b.nodeRepo.GetNode(ctx, req.End.EndPoint)
		if err != nil {
			return &DeleteEdgeResp{}, err
		}
		if end != nil {
			ends = append(ends, end)
		}

	case len(req.Lables) != 0:
	Next:
		for _, next := range start.Nexts {
			if len(next.Labels) == 0 {
				continue
			}
			for key, value := range req.Lables {
				if next.Labels[key] != value {
					continue Next
				}
			}
			end, err := b.nodeRepo.GetNode(ctx, next.EndPoint)
			if err != nil {
				return &DeleteEdgeResp{}, err
			}
			if end == nil {
				continue
			}

			ends = append(ends, end)
		}
	default:
		// nothing to do
		return &DeleteEdgeResp{}, nil
	}

	for _, end := range ends {
		for i, pre := range end.Pres {
			if pre.EndPoint == req.Start.EndPoint {
				end.Pres = append(end.Pres[:i], end.Pres[i+1:]...)
				break
			}
		}
		if err := b.updateOrdelete(ctx, end); err != nil {
			return &DeleteEdgeResp{}, err
		}
	}

	for i, next := range start.Nexts {
		for _, end := range ends {
			if next.EndPoint == end.EndPoint {
				start.Nexts = append(start.Nexts[:i], start.Nexts[i+1:]...)
				break
			}
		}
	}

	if err := b.updateOrdelete(ctx, start); err != nil {
		return &DeleteEdgeResp{}, err
	}

	return &DeleteEdgeResp{}, nil
}

func (b *Bonding) updateOrdelete(ctx context.Context, node *v1alpha1.Node) error {
	if node == nil {
		return nil
	}

	if len(node.Nexts) == 0 && len(node.Pres) == 0 {
		return b.nodeRepo.Delete(ctx, node)
	}

	return b.nodeRepo.Insert(ctx, node)
}

func (b *Bonding) getNode(ctx context.Context, endpoint v1alpha1.EndPoint) (*v1alpha1.Node, error) {
	node, err := b.nodeRepo.GetNode(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	if node == nil {
		node = &v1alpha1.Node{
			EndPoint: endpoint,
		}
	}

	return node, err
}

func mapMerge(dst, src map[string]string) map[string]string {
	if dst == nil {
		dst = make(map[string]string)
	}
	for k, v := range src {
		dst[k] = v
	}

	return dst
}
