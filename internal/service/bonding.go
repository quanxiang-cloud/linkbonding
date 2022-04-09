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

	// UID The business party should ensure that
	// the id is globally unique and can be traced back
	UID string `json:"uid,omitempty"`
}

// InsertEdgeResp insert edge resp
type InsertEdgeResp struct {
}

// TODO should break cycle?
// TODO nodes are critical resources and need to use distributed lock control.
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

	var next *v1alpha1.Edge
	uid := v1alpha1.UID + "/" + req.UID
	if next = start.GetNext(req.End.EndPoint); next == nil {
		next = &v1alpha1.Edge{
			EndPoint: req.End.EndPoint,
		}

		if len(start.Nexts) == 0 {
			start.Nexts = make([]*v1alpha1.Edge, 0)
		}
		start.Nexts = append(start.Nexts, next)

		if len(end.Pres) == 0 {
			end.Pres = make([]*v1alpha1.Edge, 0)
		}
		end.Pres = append(end.Pres, &v1alpha1.Edge{
			EndPoint: req.Start.EndPoint,
		})
	}

	if len(next.Labels) == 0 {
		next.Labels = map[string]map[string]interface{}{}
	}
	if len(next.Labels[uid]) == 0 {
		next.Labels[uid] = make(map[string]interface{})
	}

	for key, value := range req.Lables {
		next.Labels[uid][key] = value
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
	Start node   `json:"start,omitempty"`
	End   node   `json:"end,omitempty"`
	UID   string `json:"uid,omitempty"`
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

	endEndPoints := make([]v1alpha1.EndPoint, 0)
	for index, next := range start.Nexts {
		uid := v1alpha1.UID + "/" + req.UID
		if _, ok := next.Labels[uid]; ok &&
			(req.End.EndPoint == "" ||
				req.End.EndPoint == next.EndPoint) {
			// delete the specified key
			delete(next.Labels, uid)

			// delete edge
			if len(next.Labels) == 0 {
				endEndPoints = append(endEndPoints, next.EndPoint)
				start.Nexts = append(start.Nexts[:index], start.Nexts[index+1:]...)
			}
		}
	}

	for _, endPoints := range endEndPoints {
		end, err := b.nodeRepo.GetNode(ctx, endPoints)
		if err != nil {
			return &DeleteEdgeResp{}, err
		}
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

	if err := b.updateOrdelete(ctx, start); err != nil {
		return &DeleteEdgeResp{}, err
	}

	return &DeleteEdgeResp{}, nil
}

// updateOrdelete if the node is an orphan node, delete it
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
