import time

import requests
from bs4 import BeautifulSoup
import re
import chardet

site_url = "https://kudikina.ru"
map_url = "/map"
times_url = "/A"
all_bus_route = "/spb/bus/"

skiplist = ["/spb/bus/3l", "/spb/bus/7l", "/spb/bus/144", "/spb/bus/149", "/spb/bus/194", "/spb/bus/197",
            "/spb/bus/197a", "/spb/bus/318", "/spb/bus/319", "/spb/bus/320", "/spb/bus/348", "/spb/bus/349",
            "/spb/bus/350", "/spb/bus/353", "/spb/bus/354", "/spb/bus/355", "/spb/bus/359", "/spb/bus/360",
            "/spb/bus/364", "/spb/bus/485", "/spb/bus/487", "/spb/bus/489"]
none_parse_stop_coordinat = {
    ""
}


def get_stop_coordinates(url):
    full_url = site_url + url + map_url
    response = requests.get(full_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    script_tags = soup.find_all('script', type="text/javascript")
    script_tag = None

    for tag in script_tags:
        if 'drawMap' in tag.text:
            script_tag = tag
            break

    if script_tag:
        script_text = script_tag.text
        coordinates = extract_coordinates(script_text)

        return coordinates
    else:
        return []


def get_stop_times(url):
    full_url = site_url + url + times_url

    response = requests.get(full_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    stop_times = []
    for stop_div in soup.find_all('div', class_='bus-stop'):
        name = stop_div.find('a').text.strip()
        start_time = stop_div.find_next_sibling('div', class_='col-xs-12').find('span').text.strip()

        clean_name = re.sub(r"\d+\) ", "", name)
        stop_times.append({"stopName": clean_name, "startTime" : start_time})

    return stop_times


def extract_coordinates(script_text):
    matches = re.findall(r'{"name":\s*"(.*?)",\s*"lat":\s*(-?\d+\.?\d*),?\s*"long":\s*(-?\d+\.?\d*)?}', script_text)

    coordinates = {}
    for match in matches:
        name = match[0].replace("\\", "")
        coordinates[name] = [match[1], match[2]]

    return coordinates


def get_all_route_url():
    full_url = site_url + all_bus_route

    response = requests.get(full_url)
    html = response.text
    soup = BeautifulSoup(html, "html.parser")

    bus_list = []

    bus_items = soup.find_all("a", class_="bus-item bus-icon")
    for item in bus_items:
        bus_number = item.text.strip()
        bus_route = item.find("span").text.strip()
        href_link = item["href"]
        bus_list.append([bus_number, bus_route, href_link])
    return bus_list

def calculate_duration(startStop, endStop):
    startHour, startMinute = map(int, startStop.split(':'))
    endHour, endMinute = map(int, endStop.split(':'))
    return (endHour*60 + endMinute) - (startHour*60 + startMinute)


def get_saint_petersburg_bus_graph():
    routes_path = get_all_route_url()
    nodes = {}
    relationships = []
    counter = 0
    for rote in routes_path:
        url = rote[2]
        if url in skiplist:
            continue
        stop_times_and_sequaence = get_stop_times(url)
        coordinates = get_stop_coordinates(url)
        last_x = float(60)
        last_y = float(30)
        for stop in stop_times_and_sequaence:
            node = stop["stopName"]
            if nodes.get(node) is not None:
                nodes[node]["roteList"].append(rote[0])
            else:
                coordinate = coordinates.get(node)
                if(coordinate is None):
                    nodes[node] = {
                        "name": node,
                        "roteList": [rote[0]],
                        "xCoordinate": last_x + 0.00002,
                        "yCoordinate": last_y + 0.00002
                    }
                else:
                    nodes[node] = {
                        "name": node,
                        "roteList": [rote[0]],
                        "xCoordinate": float(coordinates[node][0]),
                        "yCoordinate": float(coordinates[node][1])
                    }
                    last_x = float(coordinates[node][0])
                    last_y = float(coordinates[node][1])
        for ind in range(0, len(stop_times_and_sequaence)-1):
            if nodes[stop_times_and_sequaence[ind]["stopName"]] is not None and nodes[stop_times_and_sequaence[ind+1]["stopName"]]:
                startStop = nodes[stop_times_and_sequaence[ind]["stopName"]]
                endStop = nodes[stop_times_and_sequaence[ind+1]["stopName"]]
                relationships.append({"startStop": startStop["name"],
                                      "endStop": endStop["name"],
                                      "name": startStop["name"] + " -> " + endStop["name"],
                                      "duration": calculate_duration(stop_times_and_sequaence[ind]["startTime"],
                                                                     stop_times_and_sequaence[ind+1]["startTime"]
                                                                     )
                                      })
        print(counter)
        print(url)
        counter += 1
        time.sleep(2)
    return (nodes,relationships)