package v1alpha1

type EndPoint string

const (
	UID = "resource.id"
)

type Edge struct {
	// EndPoint endpoint inique id
	EndPoint EndPoint `json:"endPoint,omitempty"`

	Labels map[string]map[string]interface{} `json:"labels,omitempty"`
}

type Node struct {
	EndPoint EndPoint `json:"endPoint,omitempty"`
	Nexts    []*Edge  `json:"nexts,omitempty"`
	Pres     []*Edge  `json:"pres,omitempty"`
}

func (n *Node) GetNext(endpoint EndPoint) *Edge {
	return n.getEdge(endpoint, n.Nexts)
}

func (n *Node) GetPre(endpoint EndPoint) *Edge {
	return n.getEdge(endpoint, n.Pres)
}

func (n *Node) getEdge(endpoint EndPoint, edges []*Edge) *Edge {
	for _, edge := range edges {
		if edge.EndPoint == endpoint {
			return edge
		}
	}

	return nil
}

type Map struct {
	Entrance Node
}
