#!/usr/bin/env python3

from __future__ import annotations

from typing import Generic, Optional, TypeVar, Protocol
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from abc import abstractmethod

import sys


C = TypeVar("C", bound="Comparable")

class Comparable(Protocol):
    @abstractmethod
    def __eq__(self, x: object) -> bool:
        pass

    @abstractmethod
    def __lt__(self: C, x: C) -> bool:
        pass



T = TypeVar('T', bound=Comparable)
V = TypeVar('V', bound=Comparable)

class Vertex(Generic[T]):
    """
    Container for a value, where all comparisons are based on the value
    """
    value: T
    links: list[Link[T]]

    @dataclass(frozen=True)
    class Link(Generic[V]):
        vertex: Vertex[V]
        weight: int = 1
        directed: bool = False


    def __init__(self, value: T, *links: Link[T]):
        """
        args:
            value: any hashable value
            links: a list of Links
        The passed in vertex will have its neighbors value updated as well 
        if undirected
        """
        self.value = value
        self.links = list(links)
        for link in links:
            self._link_check(link)


    def _link_check(self, link: Link[T]):
        if not link.directed:
            link.vertex.links.append(
                Vertex.Link(self, link.weight, link.directed)
            )

    def create_link(self, vertex: Vertex[T], weight: int, directed: bool=False):
        new_link = Vertex.Link(vertex, weight, directed)
        self.add_links(new_link)


    def add_links(self, *links: Link[T]):
        for link in links:
            self.links.append(link)
            self._link_check(link)


    def neighbors(self):
        return [link.vertex for link in self.links]

    def __eq__(self, other: Vertex[T]):
        return self.value == other.value

    def __hash__(self):
        return hash(self.value)

    def __str__(self):
        return f'{self.value}'

    def __repr__(self):
        return str(self)


class Graph(Generic[T]):
    """ Stores a graph structure """
    _vertices: list[Vertex[T]]
    _edges: dict[tuple[Vertex[T], Vertex[T]], int]


    def __init__(self, *vertices: Vertex[T]):
        """create vertex and edge lookup table"""
        # not efficient in creating the graph, O(V*E)
        unnested = (
            (vertex, link.vertex, link.weight)
            for vertex in vertices
            for link in vertex.links
        )

        unnested = sorted(unnested, key=lambda tup: tup[2], reverse=True)

        self._vertices = sorted(set(vertices), key=lambda v: v.value)
        self._edges = {
            (v1, v2): weight
            for v1, v2, weight in unnested
        }


    @staticmethod
    def from_map(map: Mapping[T, Optional[Iterable[T]]]) -> Graph[T]:
        """ Creates a graph from a connection map """
        vertices = {v: Vertex(v) for v in map.keys()}
        for node, neighbors in map.items():
            if neighbors:
                links = [Vertex.Link(vertices[n]) for n in neighbors]
                vertices[node].add_links(*links)

        return Graph(*vertices.values())


    def has_connection(self, a: T, b: T) -> bool:
        """ True if an edge exists in the graph """
        va = self.get_vertex(a)
        vb = self.get_vertex(b)
        return (va, vb) in self._edges


    def get_vertex(self, value: T):
        """ Gets the vertex associated with the value """
        value_map = {
            vertex.value: vertex
            for vertex in self._vertices
        }

        return value_map[value]


    def undirected_weight(self) -> int:
        """ Total weight of the graph, assuming each edge is undirected """
        tot_weight = 0
        for weight in self._edges.values():
            tot_weight += weight

        return int(tot_weight/2)


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


    @dataclass(repr=False)
    class MinAttrs(Generic[V]):
        """ Values kept track for finding path trees """
        key: int = sys.maxsize
        copy: Optional[Vertex[V]] = None
        parent: Optional[Vertex[V]] = None

        def __repr__(self):
            return f'{self.key}: {self.copy}'


    @staticmethod
    def _min_vertices(v_attrs: dict[Vertex[T], Graph.MinAttrs[T]]) -> list[Vertex[T]]:
        """ Finds the vertices with the minimum key in the graph """
        min_val = sys.maxsize
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


    def min_span(self) -> Graph[T]:
        """ A new graph that is a minimum span; which helps simplify graph traversal """
        start = self._vertices[0] # will be a random starting point

        # creates a dictionary of objects based on the size of the graph
        v_attrs = {vertex: Graph.MinAttrs[T]() for vertex in self._vertices}
        v_attrs[start].key = 0

        # guaranteed to have at least one vertex
        while vertex := Graph._min_vertices(v_attrs):
            vertex = vertex[0] # just get first result and ignore other options

            v_attr = v_attrs[vertex]
            v_attr.copy = Vertex(vertex.value)

            for neighbor in vertex.neighbors(): # updates adjacent vals
                n_attr = v_attrs[neighbor]
                check_val = self._edges[(vertex, neighbor)]

                if n_attr.copy is None and n_attr.key > check_val:
                    n_attr.key = check_val
                    n_attr.parent = v_attr.copy

        new_verts = list[Vertex[T]]()

        for attr in v_attrs.values():
            if not attr.copy: # will always be false, here just to get rid of warning
                continue

            new_verts.append(attr.copy)
            if attr.parent: # copies should hash the same as original
                attr.copy.create_link(attr.parent, self._edges[(attr.copy, attr.parent)])

        return Graph(*new_verts)



if __name__ == "__main__":
    # a = Vertex("a")
    # b = Vertex("b", Vertex.Link(a, 6))
    # c = Vertex("c")
    # d = Vertex('d', Vertex.Link(b, 1))
    # e = Vertex('e')
    # f = Vertex('f', Vertex.Link(a, 2), Vertex.Link(c, 6))
    # g = Vertex('g', Vertex.Link(c, 5), Vertex.Link(d, 3))
    # h = Vertex('h', Vertex.Link(d, 9), Vertex.Link(e, 8))
    # i = Vertex('i', Vertex.Link(e, 3), Vertex.Link(g, 4))

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

    span = graph.min_span()

    print(f'Minimum span tree, weight: {span.undirected_weight()}\n{span}\n')