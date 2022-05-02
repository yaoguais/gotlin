package gotlin

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/heimdalr/dag"
	"github.com/pkg/errors"
)

type InstructionVertex struct {
	WrappedID     string        `json:"i"`
	InstructionID InstructionID `json:"v"`
}

func NewInstructionVertex(id string, in InstructionID) InstructionVertex {
	return InstructionVertex{id, in}
}

func (v InstructionVertex) ID() string {
	return v.WrappedID
}

func (v InstructionVertex) Vertex() (id string, value interface{}) {
	return v.WrappedID, v.InstructionID
}

type InstructionEdge struct {
	SrcID string `json:"s"`
	DstID string `json:"d"`
}

func (e InstructionEdge) Edge() (srcID, dstID string) {
	return e.SrcID, e.DstID
}

type StorableDAG struct {
	InstructionVertices []InstructionVertex `json:"vs"`
	InstructionEdges    []InstructionEdge   `json:"es"`
}

func (d StorableDAG) Vertices() []dag.Vertexer {
	l := make([]dag.Vertexer, 0, len(d.InstructionVertices))
	for _, v := range d.InstructionVertices {
		l = append(l, v)
	}
	return l
}

func (d StorableDAG) Edges() []dag.Edger {
	l := make([]dag.Edger, 0, len(d.InstructionEdges))
	for _, v := range d.InstructionEdges {
		l = append(l, v)
	}
	return l
}

type InstructionDAG struct {
	dag *dag.DAG
	id  *int64
	m   map[InstructionID]string
}

func NewInstructionDAG() InstructionDAG {
	id := int64(0)
	return InstructionDAG{
		dag: dag.NewDAG(),
		id:  &id,
		m:   make(map[InstructionID]string),
	}
}

func ParseInstructionDAG(s string) (InstructionDAG, error) {
	d, err := dag.UnmarshalJSON([]byte(s), &StorableDAG{})
	if err != nil {
		return InstructionDAG{}, errors.Wrap(err, "Parse instruction dag")
	}

	id := int64(0)
	m := make(map[InstructionID]string)

	for i, v := range d.GetVertices() {
		m[v.(InstructionID)] = i
		i2, err := strconv.ParseInt(strings.TrimPrefix(i, "0"), 10, 64)
		if err != nil {
			return InstructionDAG{}, errors.Wrap(err, "Parse instruction dag id")
		}
		if i2 > id {
			id = i2
		}
	}

	return InstructionDAG{
		dag: d,
		id:  &id,
		m:   m,
	}, nil
}

func (d InstructionDAG) Add(ins ...InstructionID) error {
	for _, in := range ins {
		id := d.nextID()
		err := d.dag.AddVertexByID(id, in)
		if err != nil {
			return err
		}
		d.m[in] = id
	}
	return nil
}

func (d InstructionDAG) AttachChildren(parent InstructionID, children ...InstructionID) error {
	for _, child := range children {
		pid := d.m[parent]
		cid := d.m[child]
		ss, err := d.dag.GetChildren(cid)
		if err != nil {
			return err
		}
		if len(ss) != 0 {
			return errors.New("Each instruction can only have one parent instruction")
		}
		err = d.dag.AddEdge(cid, pid)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d InstructionDAG) Children(parent InstructionID) ([]InstructionID, error) {
	pid := d.m[parent]

	vertices, err := d.dag.GetParents(pid)
	if err != nil {
		return nil, err
	}
	ids := vertexIDs(vertices)

	ins := []InstructionID{}
	for _, id := range ids {
		ins = append(ins, vertices[id].(InstructionID))
	}
	return ins, nil
}

func (d InstructionDAG) Ancestors() []InstructionID {
	vertices := d.dag.GetRoots()
	rootIDs := vertexIDs(vertices)
	ids := []InstructionID{}
	for _, v := range rootIDs {
		ids = append(ids, d.ancestors(v))
	}
	return uniqueInstructionIDs(ids)
}

func (d InstructionDAG) ancestors(id string) InstructionID {
	v, _ := d.dag.GetVertex(id)
	in := v.(InstructionID)
	children, _ := d.dag.GetChildren(id)
	ids := vertexIDs(children)
	n := len(ids)
	if n > 1 {
		panic("Instruction dag is impossible")
	}
	if n == 0 {
		return in
	}
	return d.ancestors(ids[0])
}

func (d InstructionDAG) Iterator() chan InstructionID {
	c := make(chan InstructionID)

	visitor := VisitorFunc(func(v dag.Vertexer) {
		_, value := v.Vertex()
		in := value.(InstructionID)
		c <- in
	})

	go func() {
		defer close(c)
		d.dag.BFSWalk(visitor)
	}()

	return c
}

func (d InstructionDAG) MarshalString() string {
	data, err := json.Marshal(d.dag)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func (d InstructionDAG) nextID() string {
	*d.id++
	return fmt.Sprintf("%09d", *d.id)
}

func vertexIDs(vertices map[string]interface{}) []string {
	ids := make([]string, 0, len(vertices))
	for id := range vertices {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func uniqueInstructionIDs(ids []InstructionID) []InstructionID {
	l := make([]InstructionID, 0, len(ids))
	m := make(map[InstructionID]bool)
	for _, id := range ids {
		if !m[id] {
			m[id] = true
			l = append(l, id)
		}
	}
	return l
}

type VisitorFunc func(dag.Vertexer)

func (f VisitorFunc) Visit(v dag.Vertexer) {
	f(v)
}
