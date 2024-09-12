import re
import time

import requests
from bs4 import BeautifulSoup

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
        time_point = stop_div.find_next_sibling('div', class_='col-xs-12').find('span')
        if time_point is not None:
            parsed_time_point = time_point.text.strip()
            if parsed_time_point[len(parsed_time_point) - 1] == 'K':
                parsed_time_point = parsed_time_point[:-1]
        else:
            return None, False
        clean_name = re.sub(r"\d+\) ", "", name)
        stop_times.append({"stopName": clean_name, "timePoint": parsed_time_point})
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
        previous_bus_stop = None
        previous_time_point = None
        for stop in stop_times_and_sequence:
            is_new_stop = True
            bus_stop_name = stop["stopName"]
            time_point = stop["timePoint"]

            coordinate = coordinates.get(bus_stop_name)
            if coordinate is not None:
                x = float(coordinates[bus_stop_name][1])
                y = float(coordinates[bus_stop_name][0])
            else:
                x = last_x
                y = last_y

            while nodes.get(bus_stop_name) is not None:
                x_old = nodes[bus_stop_name]["xCoordinate"]
                y_old = nodes[bus_stop_name]["yCoordinate"]
                if are_stops_same((x_old, y_old), (x, y)):
                    is_new_stop = False
                    break
                else:
                    bus_stop_name = increment_suffix(bus_stop_name)

            if not is_new_stop:
                bus_stop = nodes.get(bus_stop_name)
                bus_stop["roteList"].append(rote[0])
            else:
                bus_stop = {
                    "name": bus_stop_name,
                    "roteList": [rote[0]],
                    "xCoordinate": float(x),
                    "yCoordinate": float(y),
                    "isCoordinateApproximate": coordinate is None
                }
                nodes[bus_stop_name] = bus_stop

            last_x = float(x)
            last_y = float(y)

            if previous_bus_stop is not None:
                start_stop = previous_bus_stop
                end_stop = bus_stop
                if start_stop is not None and end_stop is not None:
                    relationships.append({"startStop": start_stop["name"],
                                          "endStop": end_stop["name"],
                                          "name": start_stop["name"] + " -> " + end_stop["name"],
                                          "duration": calculate_duration(previous_time_point, time_point)
                                          })

            previous_bus_stop = bus_stop
            previous_time_point = time_point

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


def are_stops_same(stop1, stop2, tolerance=0.005):
    x1, y1 = stop1
    x2, y2 = stop2
    return abs(x1 - x2) < tolerance and abs(y1 - y2) < tolerance


def increment_suffix(name):
    if name and name[-1].isdigit():
        index = len(name) - 1
        while index >= 0 and name[index].isdigit():
            index -= 1
        number = int(name[index + 1:]) + 1
        return f"{name[:index + 1]}{number}"
    else:
        return f"{name} 1"
