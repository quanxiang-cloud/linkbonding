package elasticsearch

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"

	"github.com/olivere/elastic/v7"
	"github.com/quanxiang-cloud/linkbonding/internal/models"
	"github.com/quanxiang-cloud/linkbonding/pkg/apis/v1alpha1"
)

type node struct {
	client *elastic.Client
}

func NewNode(client *elastic.Client) models.NodeRepo {
	return &node{
		client: client,
	}
}

func (n *node) index() string {
	return "node"
}

func (n *node) Insert(ctx context.Context, node *v1alpha1.Node) error {
	id, err := md5hex([]byte(node.EndPoint))
	if err != nil {
		return err
	}
	_, err = n.client.Index().
		Index(n.index()).
		Id(id).
		BodyJson(node).
		Do(ctx)

	return err
}

func (n *node) Delete(ctx context.Context, node *v1alpha1.Node) error {
	id, err := md5hex([]byte(node.EndPoint))
	if err != nil {
		return err
	}

	_, err = n.client.Delete().
		Index(n.index()).
		Id(id).
		Do(ctx)

	return err
}

func (n *node) GetNode(ctx context.Context, endPoint v1alpha1.EndPoint) (*v1alpha1.Node, error) {
	id, err := md5hex([]byte(endPoint))
	if err != nil {
		return nil, err
	}

	result, err := n.client.Search().
		Index(n.index()).Query(
		elastic.NewTermQuery("_id", id),
	).
		Do(ctx)

	if err != nil {
		return nil, err
	}

	if len(result.Hits.Hits) == 0 {
		return nil, nil
	}

	node := new(v1alpha1.Node)
	err = json.Unmarshal(result.Hits.Hits[0].Source, node)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func md5hex(data []byte) (string, error) {
	hash := md5.New()
	_, err := hash.Write(data)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}
