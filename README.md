# Digitally identified individuals at highest risk from COVID-19 potentially eligible for non-hospital based COVID-19 treatment in England

The UK Government has made available a range of COVID-19 treatment options for non-hospitalised individuals at highest risk of severe complications from COVID-19.

This project shows how the open data statistics in the management information (MI) publications were produced.

## Useful links

### Link to publication

- [MI pubication series](https://digital.nhs.uk/data-and-information/publications/statistical/mi-digitally-identified-individuals-at-highest-risk-from-covid-19-potentially-eligible-for-non-hospital-based-covid-19-treatment-in-england)
- [Latest statistics](https://digital.nhs.uk/data-and-information/publications/statistical/mi-digitally-identified-individuals-at-highest-risk-from-covid-19-potentially-eligible-for-non-hospital-based-covid-19-treatment-in-england/current)

### Background information

- Information on who can have [treatment for COVID-19 can be found here](https://www.nhs.uk/conditions/covid-19/treatments-for-covid-19/)

## How the code works

Data stored and processed on the [Data Access Environment](https://digital.nhs.uk/services/data-access-environment-dae/user-guides). Therefore, you cannot reproduce the results without the appropriate access rights and credentials.

Data sets used are:
- Audit table of digitally identified individuals at highest risk from COVID-19 potentially eligible for non-hospital based COVID-19 treatment in England
- Person Demographics Service
- Ethnic Category

1. 01_run_notebooks creates the line-level table
2. 02_create_csv_for_national_table creates the open data csv seen in the [first (October) MI release](https://digital.nhs.uk/pubs/highestriskcovid-oct22)

### Parameters

01_run_notebooks contains the following widgets:
- input_db
- batch
- output_table_prefix

### What this code can do

- Join a table of digitally identified individuals and a snapshot of their details to other data sets to get more information.
- Aggregate line level results to produce the data in the MI publication.

### What this code does **not** do
- Cohorting of individuals from raw data sets containing GP, hospital, prescription, or other personal details.
- Look at historical information in other data sets that the snapshot is joined to - for example, the region that the individual lived in at the time they were cohorted.
- Include individuals that were referred for treatment directly by their consultant.

## Contributing

We do not accept external contributions to this GitLab.

However, we welcome feedback - please submit an issue/suggestion via Issues.

## Authors and acknowledgment
Data & Analytics, NHS England

If you have any questions, please contact datascience@nhs.net.

## Licence
[Open Government Licence v3.0](www.nationalarchives.gov.uk/doc/open-government-licence)

## Project status
Updating code for this project is low priority because it is an ad hoc publication. This repository may not match the latest release.
