import csv

revisions = [
    {
        "id": "1",
        "parent_id": None,
        "timestamp": "2017-03-19T04:23:23Z",
        "page_id": "1",
        "page_title": "nice",
        "page_ns": "1",
        "contributor_id": None,
        "contributor_name": None,
        "contributor_ip": "192.168.0.1",
        "comment": None,
        "text": "hi",
    },
    {
        "id": "2",
        "parent_id": "1",
        "timestamp": "2017-03-19T04:24:23Z",
        "page_id": "1",
        "page_title": "nice",
        "page_ns": "1",
        "contributor_id": "1",
        "contributor_name": "person",
        "contributor_ip": None,
        "comment": None,
        "text": "hi\nhi",
    },
    {
        "id": "3",
        "parent_id": None,
        "timestamp": "2017-03-19T04:25:23Z",
        "page_id": "2",
        "page_title": "also nice",
        "page_ns": "2",
        "contributor_id": "2",
        "contributor_name": "another_person",
        "contributor_ip": None,
        "comment": "sometimes there are comments",
        "text": None,
    },
]

field_names = [
    "id",
    "parent_id",
    "page_title",
    "contributor_id",
    "contributor_name",
    "contributor_ip",
    "timestamp",
    "text",
    "comment",
    "page_id",
    "page_ns",
]

with open("sample.csv", "w") as f:
    writer = csv.DictWriter(f, fieldnames=field_names)
    writer.writeheader()
    writer.writerows(revisions)