import time

import requests
from bs4 import BeautifulSoup
import re

site_url = "https://kudikina.ru"
map_url = "/map"
times_url = "/A"
all_bus_route = "bus/"
city_avg_x_coordinate = 60
city_avg_y_coordinate = 30


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
        start_time = stop_div.find_next_sibling('div', class_='col-xs-12').find('span')
        if start_time is not None:
            parsed_start_time = start_time.text.strip()
            if parsed_start_time[len(parsed_start_time) - 1] == 'K':
                parsed_start_time = parsed_start_time[:-1]
        else:
            return None, False
        clean_name = re.sub(r"\d+\) ", "", name)
        stop_times.append({"stopName": clean_name, "startTime": parsed_start_time})
    return stop_times, True


def extract_coordinates(script_text):
    matches = re.findall(r'{"name":\s*"(.*?)",\s*"lat":\s*(-?\d+\.?\d*),?\s*"long":\s*(-?\d+\.?\d*)?}', script_text)

    coordinates = {}
    for match in matches:
        name = match[0].replace("\\", "")
        coordinates[name] = [match[1], match[2]]

    return coordinates


def get_all_route_url(city_url):
    full_url = site_url + city_url + all_bus_route

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


def calculate_duration(start_stop, end_stop):
    start_hour, start_minute = map(int, start_stop.split(':'))
    end_hour, end_minute = map(int, end_stop.split(':'))
    return (end_hour * 60 + end_minute) - (start_hour * 60 + start_minute)


def get_bus_graph(city_name):
    # cities_url = parse_all_city_urls()
    city_url = "/kerch/"
    if city_url is None:
        print('No such city in parsed data')
        return None, None
    routes_path = get_all_route_url(city_url)
    nodes = {}
    relationships = []
    for rote in routes_path:
        url = rote[2]
        (stop_times_and_sequence, successes_parse) = get_stop_times(url)
        if successes_parse is False:
            continue
        coordinates = get_stop_coordinates(url)
        last_x = float(city_avg_x_coordinate)
        last_y = float(city_avg_y_coordinate)
        for stop in stop_times_and_sequence:
            node = stop["stopName"]
            if nodes.get(node) is not None:
                nodes[node]["roteList"].append(rote[0])
            else:
                coordinate = coordinates.get(node)
                if coordinate is None:
                    nodes[node] = {
                        "name": node,
                        "roteList": [rote[0]],
                        "xCoordinate": last_x,
                        "yCoordinate": last_y,
                        "isCoordinateApproximate": True
                    }
                else:
                    x = coordinates[node][1]
                    y = coordinates[node][0]
                    nodes[node] = {
                        "name": node,
                        "roteList": [rote[0]],
                        "xCoordinate": float(x),
                        "yCoordinate": float(y),
                        "isCoordinateApproximate": False
                    }
                    last_x = float(x)
                    last_y = float(y)
        for ind in range(0, len(stop_times_and_sequence) - 1):
            if nodes[stop_times_and_sequence[ind]["stopName"]] is not None and nodes[
                stop_times_and_sequence[ind + 1]["stopName"]]:
                start_stop = nodes[stop_times_and_sequence[ind]["stopName"]]
                end_stop = nodes[stop_times_and_sequence[ind + 1]["stopName"]]
                relationships.append({"startStop": start_stop["name"],
                                      "endStop": end_stop["name"],
                                      "name": start_stop["name"] + " -> " + end_stop["name"],
                                      "duration": calculate_duration(stop_times_and_sequence[ind]["startTime"],
                                                                     stop_times_and_sequence[ind + 1]["startTime"]
                                                                     )
                                      })
        print(url)
        time.sleep(2)
    return nodes, relationships


def parse_all_city_urls():
    url = "https://kudikina.ru/"
    response = requests.get(url)
    time.sleep(2)
    html_content = response.text
    soup = BeautifulSoup(html_content, 'html.parser')
    cities = {}

    for li in soup.find_all('ul', class_='list-unstyled cities block-regions'):
        for region in li.find_all('a'):
            region_name = region.find('span', class_='city-name').text.strip()
            region_href = region['href']
            region_response = requests.get(url[:-1] + region_href)
            region_html_content = region_response.text
            region_soup = BeautifulSoup(region_html_content, 'html.parser')
            city_list = region_soup.find_all('ul', class_='list-unstyled cities')
            time.sleep(2)
            if len(city_list) == 0:
                cities[region_name] = region_href
                print(region_href + ' Was parsed')
            else:
                region_cities = city_list[0].find_all('a')
                for city in region_cities:
                    city_name = city.find('span', class_='city-name').text.strip()
                    city_href = city['href']
                    cities[city_name] = city_href
                    print(city_href + ' Was parsed')
    return cities
