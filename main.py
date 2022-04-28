import configparser
import datetime
import os
import sys
import threading
import time
from calendar import monthrange

import requests
import xlsxwriter
from dateutil.relativedelta import relativedelta
from pathvalidate import sanitize_filename
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from xlsxwriter.exceptions import FileCreateError

VERSION = 1.0
session_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
SECURITY = 'd5b3c5187a96753e17451478e6798424610c0f577cf7e3141efb0fee93d56aa7'
excel_filename = ""

overall_start_time = time.time()

retry_strategy = Retry(
    total=5,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "OPTIONS"],
    backoff_factor=1
)
adapter = HTTPAdapter(max_retries=retry_strategy)
http = requests.Session()
http.mount("https://", adapter)


# http.mount("http://", adapter)


def get_time_stamp(start):
    return str(datetime.timedelta(seconds=time.time() - start)).split(".")[0]


def config_section_map(section):
    dict1 = {}
    options = config.options(section)
    for option in options:
        try:
            dict1[option] = config.get(section, option)
        except Exception as e:
            print("exception on %s! (%s)" % (option, e))
            dict1[option] = None
    return dict1


def create_folder(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        return True
    else:
        return False


def human_bytes(_bytes):
    """Return the given bytes as a human friendly KB, MB, GB, or TB string"""
    _bytes = float(_bytes)
    kb = float(1000)
    mb = float(kb ** 2)
    gb = float(kb ** 3)
    tb = float(kb ** 4)

    if _bytes < kb:
        return '{0} {1}'.format(_bytes, 'Bytes' if 0 == _bytes > 1 else 'Byte')
    elif kb <= _bytes < mb:
        return '{0:.2f} KB'.format(_bytes / kb)
    elif mb <= _bytes < gb:
        return '{0:.2f} MB'.format(_bytes / mb)
    elif gb <= _bytes < tb:
        return '{0:.2f} GB'.format(_bytes / gb)
    elif tb <= _bytes:
        return '{0:.2f} TB'.format(_bytes / tb)


def get_deployment(deployment_id):
    response = http.post("https://cloudimagedistribution.appspot.com/get_deployment",
                         {'deploymentID': deployment_id}).json()
    if response['status'] == "ok":
        return response['domain']
    else:
        return False


def get_subscriber_key(access_key, _domain):
    response = http.post(f"https://{_domain}.appspot.com/mobile/check_access_code",
                         dict(access_key=SECURITY, access_code=access_key[6::])).json()
    if response['status'] == "ok":
        return response['subscriber'], response['subscriber_key']
    else:
        return False, False


def get_transactions(_domain, subscriber_key, _start_date, _end_date, _session_id, _template_key=None,
                     _customer_key=None):
    data = {'subscriberKey': subscriber_key, 'access_key': SECURITY,
            'from_date': _start_date, 'to_date': _end_date, 'session_id': _session_id, 'templateKey': _template_key,
            'customerKey': _customer_key}
    url = f"https://{_domain}.appspot.com/desktop/transactions"

    all_tasks = []

    print("Receiving Task data")
    first = True
    more = True
    received_tasks = 0
    total_size = 0
    while more:
        # plainResponse = http.post(url, data)

        # print(plainResponse.content)

        response = http.post(url, data).json()

        total_size += response['totalSize']
        received_tasks += len(response['tasks'])
        ts = get_time_stamp(overall_start_time)
        if first:
            print(
                "Found %d tasks from %s to %s. Downloading task data..." % (
                    response['numTasks'], _start_date, _end_date))
            first = False
        print("%s: Loaded %d/%d tasks, total size: %s" % (
            ts, received_tasks, response['numTasks'], human_bytes(total_size)))

        all_tasks.extend(response['tasks'])

        if "cursor" in response:
            data['cursor'] = response['cursor']
        else:
            more = False

    print()
    all_tasks.reverse()
    return all_tasks


def log_error(errors):
    create_folder("logs")
    file_object = open('logs/error.log', 'a')
    for error in errors:
        file_object.write(error)
    file_object.close()


def download_image(image, destination_file):
    # print(image)
    try:
        # completed_images += 1
        url = image['url'] + "=s" + config_section_map("Download")["longest_side"]
        # completed_size += image['size']
        r = http.get(url, allow_redirects=True)
        open(destination_file, 'wb').write(r.content)
    except Exception as e:
        log_error([datetime.datetime.now().strftime("%d/%m/%Y %H:%M%:%S"), image + "\n", repr(e) + "\n\n"])


def download_images(deployment, base_folder, all_tasks, save_excel_task=False, save_excel_day=False):
    total_images = 0
    completed_size = 0
    total_size = 0
    completed_images = 0
    completed_tasks = 0

    # count images + size
    tasks_per_day = {}
    for task in all_tasks:
        if save_excel_day:
            ts = datetime.datetime.fromtimestamp(int(task["timestamp"]))
            local_date = ts.strftime("%Y%m%d")
            tasks_per_day.setdefault(local_date, []).append(task)
        total_images += len(task['images'])
        total_size += task['imageSize']

    if save_excel_day:
        data_folder = os.path.join(base_folder, "data")
        create_folder(data_folder)
        for i, (k, v) in enumerate(tasks_per_day.items()):
            xls_file = os.path.join(data_folder, k + ".xlsx")
            create_excel(xls_file, v)

    start_time = time.time()
    for task in all_tasks:
        # print(task)
        task_id = task['orderid']
        customer = "".join(x for x in sanitize_filename(task["customer"]) if x.isalnum())
        # template = "".join(x for x in sanitize_filename(task["template"]) if x.isalnum())
        # if template == "":
        #     template = "no_template"

        ts = datetime.datetime.fromtimestamp(int(task["timestamp"]))
        local_date = ts.strftime("%Y%m%d")
        local_time = ts.strftime("%H%M%S")
        # print(ts)

        cleaned_order_id = task_id.replace("/", ",").replace("*", "-").replace("°", "-").replace("）", ")") \
            .replace("（", ")")
        cleaned_order_id = sanitize_filename(cleaned_order_id)
        if (len(cleaned_order_id)) > 40:
            cleaned_order_id = cleaned_order_id[0: 37] + "---"
            print(task_id, cleaned_order_id)
        destination_folder = (os.path.join(base_folder, customer, local_date, cleaned_order_id))
        create_folder(destination_folder)
        if save_excel_task:
            xls_filename = os.path.join(destination_folder, cleaned_order_id + ".xlsx")
            create_excel(xls_filename, [task])
        if total_images > 0:
            percentage_done = 100 * completed_images / float(total_images)
        else:
            percentage_done = 100
        ts = get_time_stamp(overall_start_time)
        strPhotos = "photo" if len(task['images']) == 1 else "photos"
        print("%s: %d/%d (%.1f%%) : Downloading task %s to %s, (%d %s, %s)... " %
              (ts, completed_tasks + 1, len(all_tasks), percentage_done, task_id, destination_folder,
               len(task['images']), strPhotos, human_bytes(task['imageSize'])), end="", flush=True)
        log_and_print(["%s: %d/%d (%.1f%%) : Downloading task %s to %s, (%d %s, %s)... " %
                       (ts, completed_tasks + 1, len(all_tasks), percentage_done, task_id, destination_folder,
                        len(task['images']), strPhotos, human_bytes(task['imageSize']))], to_print=False)

        download_pdf(deployment, task['key'], destination_folder, cleaned_order_id)

        image_index = 1
        threads = []
        for image in task['images']:
            destination_file = os.path.join(
                destination_folder, f"{str(image_index).zfill(3)}_{cleaned_order_id}_{local_time}.jpg")
            x = threading.Thread(target=download_image, args=(image, destination_file))
            threads.append(x)
            x.start()
            image_index += 1

        completed_images += len(task['images'])
        completed_size += task['imageSize']

        for index, thread in enumerate(threads):
            thread.join()

        completed_tasks += 1
        end_time = time.time()
        download_speed = (completed_size / 1000000) / (end_time - start_time)
        if completed_images > 0:
            total_time = (total_images / float(completed_images) * (end_time - start_time))
        else:
            total_time = (end_time - start_time)
        time_remaining = total_time - (end_time - start_time)
        time_remaining_formatted = str(datetime.timedelta(seconds=time_remaining)).split(".")[0]

        print("Done (Downloaded %s @ %.1f MB/s, est time remaining: %s)" %
              (human_bytes(completed_size), download_speed, time_remaining_formatted))

    return completed_tasks, completed_images, completed_size


def create_excel(filepath, tasks):
    # excel_filename = filepath
    workbook = xlsxwriter.Workbook(filepath)
    worksheet = workbook.add_worksheet()

    header_row = ["ID", "Create Date", "Create Time", "Upload Date", "Upload Time", "Duration", "Operator", "Customer",
                  "Image Size", "Num Images", "Template", "Lat", "Lng", "Address"]

    expando_task = []

    maxNumImages = 0

    for task in tasks:
        expando_properties = {}
        for expando_property in task['expandoproperties']:
            expando_properties[expando_property] = task['expandoproperties'][expando_property]
            uc_first_property = expando_property[0].upper() + expando_property[1:].lower()
            if uc_first_property not in header_row:
                header_row.append(uc_first_property)
                expando_task.append(expando_property)

        maxNumImages = max(maxNumImages, len(task['images']))

    for i in range(0, maxNumImages):
        header_row.append(f"Image {i + 1}")

    worksheet.write_row(0, 0, header_row)
    row_index = 1

    for task in tasks:
        # create basic columns
        if "finalizedTS" in task:
            uploaded_local_date, uploaded_local_time = task['finalizedTS'].rsplit(" ", 1)
        else:
            uploaded_local_date, uploaded_local_time = " - ", " - "
        created_date, created_time = task['created'].rsplit(" ", 1)

        duration = task['duration'] if "duration" in task else "-"

        row = [task['orderid'], created_date, created_time, uploaded_local_date, uploaded_local_time, duration,
               task['operator'], task['customer'], human_bytes(task['imageSize']), str(len(task['images'])),
               task['template'], str(task["lat"]), str(task["lng"]), task["location"]]

        for expando in expando_task:
            if expando in task['expandoproperties']:
                value = task['expandoproperties'][expando]
                if isinstance(value, bool):
                    row.append("True" if value else "False")
                else:
                    row.append(str(value))
            else:
                row.append("")

        # images
        images = []
        if len(task['images']) > 0:
            for item in task['images']:
                images.append(item['url'] + "=s0")
            row.extend(images)

        worksheet.write_row(row_index, 0, row)
        row_index += 1

    xls_saved = False
    while True:
        try:
            workbook.close()
            xls_saved = True
        except FileCreateError:
            print("Error saving file " + filepath)
            input("Close file and press Enter to try again.")
        if xls_saved:
            break


def start_log():
    file_object = open(f'log_{session_id}.html', 'a')
    timestamp = datetime.datetime.now().strftime("%d %b %Y %H:%M:%S")
    file_object.write(f"<p>Log started on {timestamp}</p>")
    file_object.write("<table>")
    file_object.close()


def end_log():
    file_object = open(f'log_{session_id}.html', 'a')
    file_object.write("</table>")
    file_object.close()


def log_and_print(messages, to_print=True):
    create_folder("logs")
    file_object = open(f'logs/log_{session_id}.html', 'a')
    for message in messages:
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        message = message.encode("utf-8")
        file_object.write(f"<tr><td>{timestamp}</td><td>{message}</td></tr>")
        if to_print:
            print(str(message, 'utf-8'))
    file_object.close()


def show_templates():
    print("Getting domain", end="", flush=True)
    deployment = get_deployment(config_section_map("Subscriber")['access_code'][0: 6])
    if deployment:
        print(": %s.appspot.com" % deployment)
        print("Getting subscriber", end="", flush=True)

        subscriber, subscriber_key = get_subscriber_key(config_section_map("Subscriber")['access_code'], deployment)
        if subscriber:
            print(": %s" % subscriber)
            print()
            print("Templates:")

            response = http.post("https://%s.appspot.com/desktop/get_templates" % deployment,
                                 {'session_id': session_id, 'subscriberKey': subscriber_key,
                                  'access_key': SECURITY}).json()

            for template in response['templates']:
                print(template['key'], ": ", template['name'])


def show_customers():
    print("Getting domain", end="", flush=True)
    deployment = get_deployment(config_section_map("Subscriber")['access_code'][0: 6])
    if deployment:
        print(": %s.appspot.com" % deployment)
        print("Getting subscriber", end="", flush=True)

        subscriber, subscriber_key = get_subscriber_key(config_section_map("Subscriber")['access_code'], deployment)
        if subscriber:
            print(": %s" % subscriber)
            print()
            print("Customers:")

            response = http.post("https://%s.appspot.com/desktop/get_customers" % deployment,
                                 {'session_id': session_id, 'subscriberKey': subscriber_key,
                                  'access_key': SECURITY}).json()

            for customer in response['customers']:
                print(customer['key'], ": ", customer['name'])


def check_token(deployment, subscriber_key):
    download_token = config_section_map("Subscriber")['download_token']
    response = http.post("https://%s.appspot.com/desktop/check_download_token" % deployment,
                         {'session_id': session_id, 'subscriber_key': subscriber_key,
                          'download_token': download_token}).json()

    print(response)

    if response['status'] == "ok":
        print(": OK")
        return True
    else:
        print(response['error'])
        return False


def download_pdf(deployment, task_key, folder, task_id):
    request = http.get(f"https://{deployment}.appspot.com/subscriber/export_pdf?task_key={task_key}")
    cleaned_order_id = task_id.replace("/", ",").replace("*", "-").replace("°", "-").replace("）", ")").replace("（", ")")
    cleaned_order_id = sanitize_filename(cleaned_order_id)

    pdf_filename = os.path.join(folder, cleaned_order_id + ".pdf")
    open(pdf_filename, 'wb').write(request.content)


def download_data(_output_folder, _start_date, _end_date, _template_key, _customer_key, _delete=False,
                  _no_photos=False, _excel_output=None):
    start_log()
    log_and_print([f"SSS Downloader version {VERSION}"])

    # output_folder = config_section_map("Download")['folder']
    log_and_print([f"Setting output folder: {_output_folder}"])

    create_folder(_output_folder)

    print("Date range: %s until %s" % (_start_date.strftime("%a %d %b %Y"),
                                       _end_date.strftime("%a %d %b %Y")))

    print()
    reports = config_section_map("Data")['excel'].split(",")
    if "day" in reports:
        log_and_print(["Downloading Daily reports. Stored in %s\\data" % _output_folder])
    if "all" in reports:
        log_and_print(["Downloading Complete report. Stored in %s" % _output_folder])
    if "task" in reports:
        log_and_print(["Downloading Individual reports. Stored in each task folder"])
    print()

    print("Getting domain", end="", flush=True)
    deployment = get_deployment(config_section_map("Subscriber")['access_code'][0: 6])
    if deployment:
        print(": %s.appspot.com" % deployment)
        print("Getting subscriber", end="", flush=True)

        subscriber, subscriber_key = get_subscriber_key(config_section_map("Subscriber")['access_code'], deployment)
        if subscriber:
            print(": %s" % subscriber)
            print()
            print("Checking download token", end="", flush=True)

            if check_token(deployment, subscriber_key):
                if config_section_map("Download")["longest_side"] == "0":
                    print("Original size photos will be downloaded")
                else:
                    print("Images will be resized to have a longest side of %s px" % config_section_map("Download")[
                        "longest_side"])
                    print("Download data speed will not be correct.")
                print()

                # if _template_key:
                #     data = dict(access_key=SECURITY,
                #                 templateKey=_template_key, subscriber_key=subscriber_key)
                #     templateResult = http.post(f"https://{deployment}.appspot.com/desktop/get_templates", data).json()
                #     if templateResult['status'] == "ok":
                #         template = templateResult['templates'][0]['name']
                #     else:
                #         template = "all"
                # else:
                #     template = "all"
                #
                # if _customer_key:
                #     data = dict(access_key=SECURITY,
                #                 customerKey=_customer_key, subscriber_key=subscriber_key)
                #     templateResult = http.post(f"https://{deployment}.appspot.com/desktop/get_customers", data).json()
                #     if templateResult['status'] == "ok":
                #         customer = templateResult['customers'][0]['name']
                #     else:
                #         customer = "all"
                # else:
                #     customer = "all"

                # print(f"Template: {template}")

                print("Loading transaction list")

                all_tasks = get_transactions(deployment, subscriber_key, _start_date.strftime("%d/%m/%Y"),
                                             _end_date.strftime("%d/%m/%Y"), session_id, template_key, _customer_key)

                if "all" in config_section_map("Data")['excel'].split(","):
                    if _excel_output:
                        xls_filename = os.path.join(_output_folder, _excel_output)
                    else:
                        xls_filename = os.path.join(_output_folder,
                                                    "tasks_%s.xlsx" % datetime.datetime.now().strftime("%Y%m%d_%H%M%S"))
                    print("Saved Excel file containing all tasks to %s." % xls_filename)
                    create_excel(xls_filename, all_tasks)

                save_excel_task = "task" in config_section_map("Data")['excel'].split(",")
                save_excel_day = "day" in config_section_map("Data")['excel'].split(",")

                if not _no_photos:
                    completed_tasks, completed_images, completed_size = download_images(deployment, _output_folder,
                                                                                        all_tasks, save_excel_task,
                                                                                        save_excel_day)

                    message = "Downloaded %s tasks with in total %d photos (%s) to %s" % (
                        completed_tasks, completed_images, human_bytes(completed_size),
                        config_section_map("Download")['folder'])
                    log_and_print([message])
                    log_and_print(["Finished in %s" % get_time_stamp(overall_start_time)])

                    # delete
                    if _delete and len(all_tasks) > 0:
                        task_keys = [o['key'] for o in all_tasks]
                        task_keys = ",".join(task_keys)
                        download_token = config_section_map("Subscriber")['download_token']

                        data = dict(access_key=SECURITY,
                                    task_keys=task_keys, subscriber_key=subscriber_key, download_token=download_token)
                        response = http.post(f"https://{deployment}.appspot.com/desktop/delete_orders", data).json()

                        if response['status'] == "error":
                            log_and_print([response['error']])
                            _delete = False
                        else:
                            log_and_print(["Downloaded tasks and images will now be moved to the recycle bin"])
                else:
                    completed_tasks = 0
                    completed_images = 0
                    completed_size = 0
                    message = 'No photos downloaded'
                end_log()

                print()
                print(20 * "*")
                print(message)

                print(20 * "*")

                f = open(f'log_{session_id}.html', "r")
                log = f.read()

                data = {'session_id': session_id,
                        'subscriberKey': subscriber_key,
                        'message': message,
                        'log': log,
                        'token': config_section_map("Subscriber")['download_token'],
                        'startDownload': int(overall_start_time),
                        'endDownload': int(time.time()),
                        'numTasks': completed_tasks,
                        'numImages': completed_images,
                        'numBytes': completed_size,
                        'templateKey': template_key,
                        'customerKey': customer_key,
                        'tasksDeleted': _delete,
                        'startDate': start_date,
                        'endDate': end_date,
                        'version': VERSION,
                        'customer': "",
                        'template': ""}

                http.post("https://%s.appspot.com/desktop/finished" % deployment, data)

        else:
            print("error getting subscriber, please check the access code in the config file. Bye!")
    else:
        print("error getting domain, please check the access code in the config file. Bye!")


def get_download_setting(_args, _opts, opt, config_name, default):
    print(f"Loading {config_name} ", end="", flush=True)
    output = None
    if opt in _opts:
        try:
            output = _args[_opts.index(opt)].strip()
        except IndexError as e:
            print(f"Error reading command line arguments ({e})")
        # else:
        #     print(f"from command line: {output} ")
    if not output:
        if config_name in config_section_map("Download"):
            output = config_section_map("Download")[config_name]
            print(f"from config file: {output} ")
        else:
            output = default
            print(f"from default value: {output} ")
    return output


def get_start_end_date(_args, _opts):
    _start_date = None
    _end_date = None
    if "-s" in opts:
        try:
            _start_date = datetime.datetime.strptime(_args[_opts.index("-s")], "%Y%m%d").date()

            if "-e" in opts:
                _end_date = datetime.datetime.strptime(_args[_opts.index("-e")], "%Y%m%d").date()
            else:
                print("Read start and end date from ")
                _end_date = datetime.datetime.now().date()

        except IndexError:
            print("Error reading command line arguments)")
        except ValueError:
            print("Wrong date format")
        else:
            print(f"from command line: {_start_date} ")
    elif "-d" in opts:
        # -d 0 means today
        # -d 1 means yesterday
        # -d 7 means 7 days ago
        try:
            _days_ago = _args[_opts.index("-d")]
        except IndexError:
            print("Error reading command line arguments)")
        else:
            print(f"from command line: {_days_ago} ")
            _end_date = datetime.datetime.now().date() - datetime.timedelta(days=int(_days_ago))  # yesterday
            _start_date = _end_date - datetime.timedelta(days=int(_days_ago))
    elif "-w" in opts:
        # -w 0 means current week
        # -w 1 means last week
        try:
            _weeks_ago = _args[_opts.index("-w")]
        except IndexError:
            print("Error reading command line arguments)")
        else:
            print(f"from command line: {_weeks_ago} ")
            # if w 0, then this week, that
            _start_date = (datetime.datetime.today() - datetime.timedelta(
                days=datetime.datetime.today().isoweekday() % 7 + (7 * int(_weeks_ago)) - 1)).date()
            _end_date = _start_date + datetime.timedelta(days=6)

    elif "-m" in opts:
        # -w 0 means current month
        # -w 1 means last month
        try:
            _months_ago = _args[_opts.index("-m")]
        except IndexError:
            print("Error reading command line arguments)")
        else:
            print(f"from command line: {_months_ago} ")
            # if w 0, then this week, that
            _start_date = (datetime.datetime.today().replace(day=1) - relativedelta(months=int(_months_ago))).date()
            _end_date = _start_date.replace(day=monthrange(_start_date.year, _start_date.month)[1])

    if not _start_date:
        # read from settings
        if "start_date" in config_section_map("Download"):
            _start_date = datetime.datetime.strptime(config_section_map("Download")["start_date"], "%d/%m/%Y").date()
            # _start_date = datetime.datetime.strptime(_args[_opts.index("-e"), "%Y%m%d"])
            if "end_date" in config_section_map("Download"):
                _end_date = datetime.datetime.strptime(config_section_map("Download")["end_date"], "%d/%m/%Y").date()
            else:
                _end_date = _start_date
        else:
            _start_date = datetime.datetime.today().date()
            _end_date = datetime.datetime.today().date()

    return _start_date, _end_date


if __name__ == "__main__":
    """ This is executed when run from the command line """

    # Load config
    config = configparser.ConfigParser()
    config.read("settings.cfg")

    # Command Line parameters
    opts = [opt for opt in sys.argv[1:] if opt.startswith("-")]
    args = [arg for arg in sys.argv[1:] if not arg.startswith("-")]

    if "-h" in opts:
        print("""Usage:
    python main.py -h:                  Shows this help message
    python main.py -templates           Shows the list of available templates
    python main.py -customers           Shows the list of available customers
    python main.py -f c:\\temp\\SSS     Set destination folder to c:\\temp\\SSS
    python main.py -t TEMPLATEKEY       Set the template key (e.g. ahdzfnNob290...UYgICAgKG_nAkM)
    python main.py -s yyyymmdd          Set the start date 
    python main.py -e yyyymmdd          Set the end date
    python main.py -s 20210101 -e 20210531  Download date range from 1 Jan 2021 to 31 May 2021
    python main.py -d 0                 Set the duration to current day
    python main.py -d 1                 Set the download period to yesterday
    python main.py -m 1 --delete        Download last month data and move all downloaded tasks to SSS Recycle Bin
    python main.py -m 2 --delete        Download two months ago data and move all downloaded tasks to SSS Recycle Bin
    python main.py -nophotos            Download Excel data only, skip photo download
    python main.py -output output.xlsx  Custom Excel output file name

    """)

    elif "-templates" in opts:
        show_templates()
    elif "-customers" in opts:
        show_customers()

    else:
        # load settings
        output_folder = get_download_setting(args, opts, "-f", "folder", "C:\\temp")
        template_key = get_download_setting(args, opts, "-t", "template_key", None)
        customer_key = get_download_setting(args, opts, "-c", "customer_key", None)

        if "-nophotos" in opts:
            no_photos = True
        else:
            no_photos = False

        excel_output = get_download_setting(args, opts, "-output", "excel_output", None)

        start_date, end_date = get_start_end_date(args, opts)

        delete = "--delete" in opts

        download_data(output_folder, start_date, end_date, template_key, customer_key, delete, no_photos, excel_output)
