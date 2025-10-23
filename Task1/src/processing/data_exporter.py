import json
import xml.etree.ElementTree as ET
from decimal import Decimal
from datetime import date, datetime
import logging

class DataExporter:
    """Класс для экспорта данных в различные форматы."""
    def __init__(self):
        self.serializers = {
            "json": self._dump_json,
            "xml": self._dump_xml
        }

    def dump_data(self, data, format: str, filename: str):
        """Вызывает определенную функцию сохранения в файл."""
        if format not in self.serializers:
            raise ValueError(f"Unsupported format: {format}")
        self.serializers[format](data, filename)

    def _serialize_data(self, obj):
        if isinstance(obj, (Decimal, date, datetime)):
            return str(obj)
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

    def _dump_json(self, data, filename: str):
        with open(f"output/{filename}.json", 'w', encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=self._serialize_data)
        logging.info(f'Query result saved in output/{filename}.json')

    def _dump_xml(self, data, filename: str):
        root = ET.Element("result")

        def create_item_element(parent, item):
            item_el = ET.SubElement(parent, "item")
            if isinstance(item, dict):
                for k, v in item.items():
                    ET.SubElement(item_el, k).text = str(v)
            else:
                ET.SubElement(item_el, "value").text = str(item)

        if isinstance(data, dict):
            for key, items in data.items():
                section = ET.SubElement(root, key)
                for item in items:
                    create_item_element(section, item)
        elif isinstance(data, list):
            section = ET.SubElement(root, "items")
            for item in data:
                create_item_element(section, item)
        else:
            raise TypeError("Data must be a dict or list")

        tree = ET.ElementTree(root)
        tree.write(f"output/{filename}.xml", encoding="utf-8", xml_declaration=True)
        logging.info(f'Query result saved in output/{filename}.xml')