# Zuora batch source

Description
-----------
Plugin fetches data from Zuora periodically. Firstly it gets all objects
of specified type, than only newly created.
 
Zuora is a subscription management platform designed to meet your
modern order-to-cash needs.
 
Properties
----------
### General

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Authentication type:** Type of authentication to the Zuora account.

**Basic:**

- ***Username:*** Login name for the Zuora account.
- ***Password:*** Password for the Zuora account.

**OAuth2:**
- ***Client ID:*** The OAuth2 client id.
- ***Client SecretD:*** The OAuth2 client secret.

**Base Objects:** Objects to fetch.

Available:
- Products
- Refunds
- PaymentRun
- Orders
- Entities
- DebitMemos
- CreditMemos
- BillingDocuments

**Pull frequency:** Delay between polling of Zuora API for updates.








