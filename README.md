### Grocery Pipeline Goals

- Incorporate slack messaging into Dagster workflow []
- Monitor GCP costs for the coming weeks -> Ongoing
- Expand locations to more cities and more stores per city []
    - Jobs takes about 15 minutes for 5 stores
    - How many per city?
    - How many per zipcode?
- Explore bronze ingested exploration data []
    - Breadth
    - Drilldown

- Get Walmart API Credentials [X]
- Look for Albertson's API []
- Look for Whole Foods API []
- Explore Walmart API []
- Look into Webscraping alternatives []

- Finalize Silver fact and dimension schemas []
    - Check to see if the data in Silver Exploration is free of false positives

---

#### Rough Outline of things to be done:
- Make sure that all data that should be in silver, is in silver [DONE]
    - Due: Friday Night 01/29
- Make sure there are no False Positives in Silver [DONE]
    - Due: Sunday Night 02/01
- Integrate Walmart API [ ]
    - Finalize Canonical Configs
        - search_terms
        - rules
    - explore
    - implement parent/child client
    - Due: 02/08

- Finalize Bronze Daily table [ ]
    - Due: 02/11
- Finalize Silver Daily table [ ]
    - Due: 02/15
- Add Slack updates [ ]
    - Due:  02/15
- Finalize README [ ]
    - Due: 2/16



