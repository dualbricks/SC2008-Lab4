# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import pandas
import requests
import networkx as nx
from pyvis.network import Network
df = pandas.read_csv('data.csv')

column_names = ['Type', 'sflow_agent_address', 'inputPort', 'outputPort',
                'src_MAC', 'dst_MAC', 'ethernet_type', 'in_vlan', 'out_vlan',
                'src_IP', 'dst_IP', 'IP_protocol', 'ip_tos', 'ip_ttl', 'udp_src_port/tcp_src_port/icmp_type',
                'udp_dst_port/tcp_dst_port/icmp_code', 'tcp_flags', 'packet_size', 'IP_size',
                'sampling_rate', 'random']

df.columns = column_names

df = df.drop(columns=['random'])


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


def Top_5_Talkers(df):
    """
    Find the Top 5 senders
    :param df: dataframe
    :return: None
    """
    top_5_talkers = df['src_IP'].value_counts().nlargest(5)

    for index, ip in enumerate(top_5_talkers.index):
        response = requests.get('http://ip-api.com/json/' + str(ip))
        org = response.json()['org']
        print(str(index) + '. ip: ' + str(ip) + ', packets: ' + str(top_5_talkers[ip]) + ', organisation: ' + str(org))


def Top_5_Listener(df):
    """
    Find the top 5 receivers
    :param df: dataframe
    :return: None
    """
    top_5_listeners = df['dst_IP'].value_counts().nlargest(5)
    for index, ip in enumerate(top_5_listeners.index):
        response = requests.get('http://ip-api.com/json/' + str(ip))
        org = response.json()['org']
        print(
            str(index) + '. ip: ' + str(ip) + ', packets: ' + str(top_5_listeners[ip]) + ', organisation: ' + str(org))


def Proportion_TCP_UDP(df):
    """
    Count the number of TCP/UDP packets and calculate their %
    :param df: dataframe
    :return: None
    """
    total_packets = df['IP_protocol'].count()
    count_TCP_UDP = df['IP_protocol'].value_counts()
    UDP_count = count_TCP_UDP[17]
    TCP_count = count_TCP_UDP[6]

    print('Total TCP packets: ' + str(TCP_count) + str('(' + str((TCP_count / total_packets) * 100) + '%)'))
    print('Total UDP packets: ' + str(UDP_count) + str('(' + str((UDP_count / total_packets) * 100) + '%)'))


def application_protocol(df):
    """
    Find the top ports used
    :param df: dataframe
    :return: None
    """
    destination_ip_port = df['udp_dst_port/tcp_dst_port/icmp_code'].value_counts().nlargest(5)

    for index, port in enumerate(destination_ip_port.index):
        print(str(index) + '.' + 'port: ' + str(port) + ', packets: ' + str(destination_ip_port[port]))


def traffic_size(df):
    """
    Calculate the traffic size assuming the packets are in bytes
    :param df: dataframe
    :return: None
    """
    size = df['IP_size'].sum()
    # tentative  dk if what units
    estimated_size = (size * 2048) * (1 * (10 ** (-6)))
    print('estimated size: ' + str(estimated_size))


def top5_comms(df):
    """
    Find the top 5 communicators in the data
    :param df: dataframe
    :return: None
    """
    top5_group = df[['src_IP', 'dst_IP']].value_counts().nlargest(5)
    for index, ips in enumerate(top5_group.index):
        res_src = requests.get('http://ip-api.com/json/' + str(ips[0]))
        org_src = res_src.json()['org']
        res_dst = requests.get('http://ip-api.com/json/' + str(ips[1]))
        org_dst = res_dst.json()['org']
        print(
            str(index) + '. src_ip: ' + str(ips[0]) + ', src_name:' + str(org_src) + '. dst_ip: ' + str(ips[1])
            + ', dst_name:' + str(org_dst) + ', packets: ' + str(top5_group[ips]))


def graph_communication(df, n):
    """
    Plot out the graph of communications
    :param df: dataframe
    :param n: network size
    :return: None
    """
    comms_list = df[['src_IP', 'dst_IP']].value_counts().nlargest(n)
    df1 = comms_list.reset_index()
    df1 = df1.rename(columns={0: 'count'})
    G = nx.from_pandas_edgelist(df1, source='src_IP', target='dst_IP', edge_attr='count')
    net = Network(directed=True, notebook=True)
    net.from_nx(G)
    net.show("graph.html")

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print("--------TOP 5 TALKERS-----------")
    print(Top_5_Talkers(df))
    print("--------TOP 5 LISTENERS---------")
    print(Top_5_Listener(df))
    print("--------TRANSPORT PROTOCOL----------")
    print(Proportion_TCP_UDP(df))
    print("--------APPLICATION PROTOCOL-----------")
    print(application_protocol(df))
    print("TRAFFIC SIZE")
    print(traffic_size(df))
    print("--------TOP 5 COMMS-----------")
    print(top5_comms(df))
    print("--------GENERATE GRAPH-----------")
    print(graph_communication(df,  300))

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
