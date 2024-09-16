import datetime
import json
import math
import os
import re
import time

import requests
from bs4 import BeautifulSoup

cache_file = './cache/city_urls.json'
cache_expire_days = 30
site_url = "https://kudikina.ru"
map_url = "/map"
timetable_forward_url = "/A"
timetable_backward_url = "/B"
all_bus_route = "bus/"
# TODO: need to update according to city coordinates
city_avg_x_coordinate = 60.0
city_avg_y_coordinate = 30.0
request_pause_sec = 2


class BusGraphParser:
    def __init__(self, city_name):
        self.city_name = city_name
        self.city_url = self.__get_city_url()
        self.all_routes_info = self.__get_all_routes_info()
        self.nodes = {}
        self.relationships = []

    def parse(self):
        if self.city_url is None:
            return None, None

        for route_info in self.all_routes_info:
            route_name = route_info[0]
            route_url = route_info[2]

            self.__add_stops_and_routes(route_name, route_url)

            print(route_url)
            time.sleep(request_pause_sec)

        return self.nodes, self.relationships

    def __get_city_url(self):
        cities_url = load_cache(cache_file)
        if not cities_url:
            print("Cities url cache is expired or empty, lets fill it.")
            cities_url = parse_all_city_urls()
            save_cache(cache_file, cities_url)
            print("Cities url are saved in cache.")
        city_url = cities_url.get(self.city_name)
        if city_url is None:
            print('No such city in parsed data')
        return city_url

    def __add_stops_and_routes(self, route_name, route_url):
        (timetable, successes_parse) = get_timetable(route_url)
        if successes_parse is False:
            return

        stop_coordinates = get_stop_coordinates(route_url)
        last_coordinate = Coordinate(city_avg_x_coordinate, city_avg_y_coordinate)
        previous_bus_stop = None
        previous_time_point = None

        for row in timetable:
            bus_stop_name = row["stopName"]
            time_point = row["timePoint"]
            coordinate = self.__get_filled_coordinate(stop_coordinates, bus_stop_name, last_coordinate)

            bus_stop = self.__update_or_add_stop(bus_stop_name, coordinate, route_name)

            if previous_bus_stop is not None:
                self.__add_route(previous_bus_stop, bus_stop, previous_time_point, time_point)

            last_coordinate = coordinate
            previous_bus_stop = bus_stop
            previous_time_point = time_point

    def __get_filled_coordinate(self, stop_coordinates, stop_name, last_coordinate):
        coordinate = stop_coordinates.get(stop_name)
        if coordinate is None or not coordinate.is_defined():
            coordinate = Coordinate(last_coordinate.x, last_coordinate.y, True)
        return coordinate

    def __update_or_add_stop(self, bus_stop_name, coordinate, route_name):

        bus_stop_name, is_new_stop = self.__check_and_find_unique_stop(bus_stop_name, coordinate)

        if not is_new_stop:
            bus_stop = self.nodes.get(bus_stop_name)
            bus_stop["roteList"].append(route_name)
        else:
            bus_stop = {
                "name": bus_stop_name,
                "roteList": [route_name],
                "xCoordinate": coordinate.x,
                "yCoordinate": coordinate.y,
                "isCoordinateApproximate": coordinate.is_approximate
            }
            self.nodes[bus_stop_name] = bus_stop
        return bus_stop

    def __check_and_find_unique_stop(self, bus_stop_name, coordinate):
        is_new_stop = True
        while self.nodes.get(bus_stop_name) is not None:
            bus_stop = self.nodes[bus_stop_name]
            old_coordinate = Coordinate(bus_stop["xCoordinate"], bus_stop["yCoordinate"])
            if are_stops_same(old_coordinate, coordinate):
                is_new_stop = False
                break
            else:
                bus_stop_name = increment_suffix(bus_stop_name)

        return bus_stop_name, is_new_stop

    def __add_route(self, start_stop, end_stop, start_time, end_time):
        if start_stop is not None and end_stop is not None:
            self.relationships.append({"startStop": start_stop["name"],
                                       "endStop": end_stop["name"],
                                       "name": start_stop["name"] + " -> " + end_stop["name"],
                                       "duration": calculate_duration(start_time, end_time)
                                       })

    def __get_all_routes_info(self):
        if self.city_url is None:
            return []

        full_url = site_url + self.city_url + all_bus_route

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


class Coordinate:
    def __init__(self, x=None, y=None, is_approximate=False):
        self.x = x
        self.y = y
        self.is_approximate = is_approximate

    def __str__(self):
        return f"({self.x}, {self.y})"

    def is_defined(self):
        if self.x is None or self.y is None:
            return False
        else:
            return True

    def get_xy(self):
        return [self.x, self.y]


def load_cache(cache_file):
    if os.path.exists(cache_file):
        modification_time = os.path.getmtime(cache_file)
        current_time = datetime.datetime.now()
        if (current_time - datetime.datetime.fromtimestamp(modification_time)).days <= cache_expire_days:
            with open(cache_file, 'r') as file:
                return json.load(file)
    return {}


def save_cache(cache_file, cache_data):
    with open(cache_file, 'w') as file:
        json.dump(cache_data, file)


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


def get_timetable(route_url):
    (timetable1, successes_parse1) = get_one_direction_timetable(route_url, timetable_forward_url)
    (timetable2, successes_parse2) = get_one_direction_timetable(route_url, timetable_backward_url)
    if successes_parse1 and successes_parse2:
        return timetable1 + timetable2, True
    else:
        return None, False


def get_one_direction_timetable(route_url, timetable_url):
    full_url = site_url + route_url + timetable_url

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


def get_stop_coordinates(route_url):
    full_url = site_url + route_url + map_url
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
        return {}


def extract_coordinates(script_text):
    matches = re.findall(r'{"name":\s*"(.*?)",\s*"lat":\s*(-?\d+\.?\d*),?\s*"long":\s*(-?\d+\.?\d*)?}', script_text)

    coordinates = {}
    for match in matches:
        name = match[0].replace("\\", "")
        # `match` contains latitude and longitude which equals to y and x coordinates
        x = float(match[2])
        y = float(match[1])
        coordinates[name] = Coordinate(x, y)

    return coordinates


def calculate_duration(start_stop, end_stop):
    start_hour, start_minute = map(int, start_stop.split(':'))
    end_hour, end_minute = map(int, end_stop.split(':'))
    return (end_hour * 60 + end_minute) - (start_hour * 60 + start_minute)


def are_stops_same(coord1, coord2, tolerance=0.005):
    distance = math.dist(coord1.get_xy(), coord2.get_xy())
    return abs(distance) < tolerance


def increment_suffix(name):
    if name and name[-1].isdigit():
        index = len(name) - 1
        while index >= 0 and name[index].isdigit():
            index -= 1
        number = int(name[index + 1:]) + 1
        return f"{name[:index + 1]}{number}"
    else:
        return f"{name} 1"
