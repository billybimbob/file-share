#!/usr/bin/env python3

from __future__ import annotations

from typing import Any, NamedTuple, Optional, TypeVar
from collections.abc import Iterable, Sequence, Mapping
from dataclasses import dataclass

from sys import maxsize


T = TypeVar('T')

class Vertex:
    """
    Container for a value, where all comparisons are based on the value
    """
    value: Any
    links: list[Link]

    class Link(NamedTuple):
        vertex: Vertex
        weight: int = 1
        directed: bool = False


    def __init__(self, value: Any, *links: Link):
        """
        args:
            value: any hashable value
            links: a list of Links
        the passed in vertex will have its neighbors value updated as well if undirected
        """
        self.value = value
        self.links = list(links)
        for link in links:
            self._link_check(link)


    def _link_check(self, link: Link):
        if not link.directed:
            link.vertex.links.append(
                Vertex.Link(self, link.weight, link.directed)
            )

    def create_link(self, vertex: Vertex, weight: int, directed: bool=False):
        new_link = Vertex.Link(vertex, weight, directed)
        self.add_links(new_link)


    def add_links(self, *links: Link):
        for link in links:
            self.links.append(link)
            self._link_check(link)


    def neighbors(self):
        return [link.vertex for link in self.links]

    def __eq__(self, other: Vertex):
        return self.value == other.value

    def __hash__(self):
        return hash(self.value)

    def __str__(self):
        return f'{self.value}'

    def __repr__(self):
        return str(self)


class Graph:
    """ Stores a graph structure """
    _vertices: list[Vertex]
    _edges: dict[tuple[Vertex, Vertex], int]

    def __init__(self, *vertices: Vertex):
        """create vertex and edge lookup table"""
        # not efficient in creating the graph, O(V*E)
        unnested = sorted(
            (
                (vertex, link.vertex, link.weight)
                for vertex in vertices
                for link in vertex.links
            ),
            key=lambda tup: tup[2],
            reverse=True
        )

        self._vertices = sorted(set(vertices), key=lambda vert: vert.value)
        self._edges = {
            (v1, v2): weight
            for v1, v2, weight in unnested
        }


    @staticmethod
    def from_map(map: Mapping[T, Optional[Iterable[T]]]) -> Graph:
        """ Creates a graph from a connection map """
        vertices = {v: Vertex(v) for v in map.keys()}
        for node, neighbors in map.items():
            if neighbors:
                links = [Vertex.Link(vertices[n]) for n in neighbors]
                vertices[node].add_links(*links)

        return Graph( *vertices.values() )


    def __str__(self):
        str_verts = [str(vertex) for vertex in self._vertices]
        ret = [' '.join([' '] + str_verts)]

        for i, a in enumerate(self._vertices):
            weights = [str_verts[i]]
            weights += [
                str(self._edges[(a, b)])
                if (a, b) in self._edges else
                '0' 
                for b in self._vertices
            ]

            ret.append(' '.join(weights))

        return '\n'.join(ret)


    def __repr__(self):
        return str(self)

    def __len__(self):
        return len(self._vertices)


    def undirected_weight(self):
        tot_weight = 0
        for weight in self._edges.values():
            tot_weight += weight

        return int(tot_weight/2)


    @dataclass(repr=False)
    class MinAttrs:
        """ Values kept track for finding path trees """
        key: int = maxsize
        copy: Optional[Vertex] = None
        parent: Optional[Vertex] = None

        def __repr__(self):
            return f'{self.key}: {self.copy}'


    @staticmethod
    def _min_vertices(v_attrs: dict[Vertex, Graph.MinAttrs]) -> list[Vertex]:
        """ Finds the vertices with the minimum key in the graph """
        min_val = maxsize
        min_verts = list[Vertex]()

        for vertex, attrs in v_attrs.items():
            if attrs.copy is not None:
                continue

            if attrs.key < min_val:
                min_val = attrs.key
                min_verts.clear()

            if attrs.key == min_val:
                min_verts.append(vertex)

        return min_verts


    def shortest_path(self, start: Vertex) -> Sequence[Any]:
        """ Calculates a shortest path from the start vertex """
        # creates a dictionary of objects based on the size of the graph
        v_attrs = {vertex: Graph.MinAttrs() for vertex in self._vertices}
        v_attrs[start].key = 0
        end = start

        # guaranteed to have at least one vertex
        while vertex := Graph._min_vertices(v_attrs):
            vertex = vertex[0] # just get first result and ignore other options
            v_attrs[vertex].copy = vertex

            for neighbor in vertex.neighbors(): # updates adjacent vals
                n_attr = v_attrs[neighbor]
                check_val = v_attrs[vertex].key + self._edges[(vertex, neighbor)]

                if n_attr.copy is None and n_attr.key > check_val:
                    n_attr.key = check_val
                    n_attr.parent = v_attrs[vertex].copy

            end = vertex

        path = list[Vertex]()
        trace: Optional[Vertex] = end

        while trace:
            attr = v_attrs[trace]
            if not attr.copy: # will always be false, here just to get rid of warning
                continue

            path.append(attr.copy)
            trace = attr.parent
            # if attr.parent: # copies should hash the same as original
            #     attr.copy.create_link(attr.parent, self.edges[(attr.copy, attr.parent)])

        return [p.value for p in reversed(path)]



if __name__ == "__main__":
    a = Vertex("a")
    b = Vertex("b", Vertex.Link(a, 6))
    c = Vertex("c")
    d = Vertex('d', Vertex.Link(b, 1))
    e = Vertex('e')
    f = Vertex('f', Vertex.Link(a, 2), Vertex.Link(c, 6))
    g = Vertex('g', Vertex.Link(c, 5), Vertex.Link(d, 3))
    h = Vertex('h', Vertex.Link(d, 9), Vertex.Link(e, 8))
    i = Vertex('i', Vertex.Link(e, 3), Vertex.Link(g, 4))

    conns = {
        "a": None,
        "b": {"a"},
        "c": None,
        "d": {"b"},
        "e": None,
        "f": {"a", "c"},
        "g": {"c", "d"},
        "h": {"d", "e"},
        "i": {"e", "g"}
    }

    # graph = Graph(a, b, c, d, e, f, g, h, i)
    graph = Graph.from_map(conns)

    print(f'Starting graph, weight: {graph.undirected_weight()}\n{graph}\n')

    shortest_a = graph.shortest_path(h)
    print(f'Shortest path tree starting from vertex "{h}", {shortest_a}\n')

    # print("If there is no unique MST, all variations will be printed")
    # min_spans = graph.min_span()
    # for min_span in min_spans:
    #     print(f'Minimum span tree, weight: {min_span.undirected_weight()}\n{min_span}\n')