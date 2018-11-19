from collections import namedtuple
import json
import networkx as nx
import pendulum


def _sane_weekday(d):
    # Map day of week to {Sun: 0, Sat: 6} instead of python's crazy
    # idea that Monday is 0
    d += 1
    if d < 7:
        return d
    return 0


Message = namedtuple('Message', ['src', 'dest', 'date', 'time', 'weekday'])


class _UserMut:
    def __init__(self, id):
        self.id = id
        self.received = []
        self.sent = []

    def add_received(self, m):
        self.received.append(m)

    def add_sent(self, m):
        self.sent.append(m)

    def to_immutable(self):
        cr = len(self.received)
        cs = len(self.sent)
        c_all = cr + cs
        return User(self.id, self.received, self.sent, c_all, cr, cs)


User = namedtuple(
        'User',
        [
            'id', 'received', 'sent', 'count_all', 'count_received',
            'count_sent'
        ]
)


def read_data(fname):
    msgs = []
    with open(fname) as fd:
        for line in fd:
            # Data format is:
            # <source user id> <dest. user id> <unix timestamp>
            src, dest, t = [int(v) for v in line.strip().split()]
            t = pendulum.from_timestamp(t).in_tz('US/Pacific')
            date = t.date()
            time = t.time()
            day = _sane_weekday(t.weekday())
            m = Message(src, dest, date, time, day)
            msgs.append(m)
    return msgs


def pivot_on_users(msgs):
    users = {}
    for m in msgs:
        fr, to, d, t, wd = m
        for i in (fr, to):
            if i not in users:
                users[i] = _UserMut(i)
        users[fr].sent.append(m)
        users[to].received.append(m)
    user_ids = list(users.keys())
    users = {k: v.to_immutable() for k, v in users.items()}
    return user_ids, users


def _create_d3_node(n, d=None):
    if d is None:
        return {'id': n}
    return {'id': n, 'degree': d}


def _create_d3_link(edge):
    return {'source': edge[0], 'target': edge[1]}


def split_by_time_of_day(msgs):
    hours = {i: None for i in range(24)}
    graphs = {i: nx.Graph() for i in range(24)}
    for m in msgs:
        graphs[m.time.hour].add_edge(m.src, m.dest)
    for h, g in graphs.items():
        nodes = list(g.nodes)
        nodes.sort()
        nodes = [_create_d3_node(n) for n in nodes]
        edges = [_create_d3_link(e) for e in g.edges]
        d = {'nodes': nodes, 'links': edges}
        hours[h] = d
    return hours, graphs


def split_by_day_of_week(msgs):
    days = {i: None for i in range(7)}
    graphs = {i: nx.Graph() for i in range(7)}
    for m in msgs:
        graphs[m.weekday].add_edge(m.src, m.dest)
    for d, g in graphs.items():
        nodes = list(g.nodes)
        nodes.sort()
        nodes = [_create_d3_node(n, g.degree[n]) for n in nodes]
        edges = [_create_d3_link(e) for e in g.edges]
        ne = {'nodes': nodes, 'links': edges}
        days[d] = ne
    return days, graphs


def dump_d3_data_json_day(msgs, fname, indent=None):
    days, gs = split_by_time_of_day(msgs)
    dump_d3_data_json(days, fname, indent)


def dump_d3_data_json_hour(msgs, fname, indent=None):
    hours, gs = split_by_time_of_day(msgs)
    dump_d3_data_json(hours, fname, indent)


def dump_d3_data_json(data, fname, indent=None):
    with open(fname, 'w') as fd:
        json.dump(data, fd, indent=indent)
