#!/usr/bin/env python
# coding: utf-8

# In[24]:


import pandas as pd
import numpy as np
import pickle

def int_convert(value, default=0):
    replacements = {'O': '0', 'o': '0', 'I': '1', 'l': '1', 'S': '5', 's': '5'}
    if isinstance(value, str):
        value = replacements.get(value, value)
    try:
        return int(value)
    except ValueError:
        return default


with open("invoices_new.pkl", "rb") as file:
    invoices_data = pickle.load(file)
with open("expired_invoices.txt", "r") as file:
    expired_invoices = set(file.read().split(", "))
    


# In[29]:


class DataExtractor:
    def __init__(self, invoices, expired_ids):
        self.invoices = invoices
        self.expired_ids = expired_ids
        self.type_conversion = {0: 'Material', 1: 'Equipment', 2: 'Service', 3: 'Other'}

    def extract_data(self):
        flat = []
        for invoice in self.invoices:
            flat.extend(self.process_invoice(invoice))
        return pd.DataFrame(flat).sort_values(by=['invoice_id', 'invoiceitem_id'])

    def process_invoice(self, invoice):
        invoice_id = invoice.get('id')
        created_on = pd.to_datetime(invoice.get('created_on'), errors='coerce')
        invoice_total = sum(int_convert(item['item'].get('unit_price')) * int_convert(item.get('quantity'))
                            for item in invoice.get('items', []))
        return [self.process_item(item, invoice_id, created_on, invoice_total) for item in invoice.get('items', [])]


    def process_item(self, item, invoice_id, created_on, invoice_total):
        item_id = item['item'].get('id')
        item_name = item['item'].get('name')
        unit_price = int_convert(item['item'].get('unit_price'))
        item_type = self.type_conversion.get(int_convert(item['item'].get('type'), 3), 'Other')
        quantity = int_convert(item.get('quantity'))
        total_price = unit_price * quantity
        percentage_in_invoice = (total_price / invoice_total) if invoice_total else 0
        is_expired = invoice_id in self.expired_ids

        return {
            'invoice_id': invoice_id,
            'created_on': created_on,
            'invoiceitem_id': item_id,
            'invoiceitem_name': item_name,
            'type': item_type,
            'unit_price': unit_price,
            'total_price': total_price,
            'percentage_in_invoice': percentage_in_invoice,
            'is_expired': is_expired
        }



extractor = DataExtractor(invoices_data, expired_invoices)
extracted_data = extractor.extract_data()
extracted_data.head()


# In[31]:


extracted_data.to_csv('result.csv', index=False)


# In[ ]:




