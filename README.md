# State Law Scraper

This repo contains a scraper for collecting the relevant state **codes** and **regulations** from Justia.

There are two scrapers available:
- `scraper.py`: A simple, single-threaded scraper for one state at a time.
- `ms.py`: A multi-threaded scraper capable of scraping multiple states in parallel, with progress bars and the ability to resume interrupted downloads.

## `scraper.py` Usage

To download the code for a single state, use 
```bash
> python scraper.py CA
```

To download regulations, we use:
```bash
> python scraper.py CA -r
```

## `ms.py` (Multi-threaded Scraper) Usage

It is recommended to use `ms.py` for scraping multiple states or for large states.

### Scraping Specific States
To download codes for multiple states in parallel:
```bash
python ms.py --states CA TX NY
```

### Scraping a Range of States
To scrape a range of states (alphabetically):
```bash
python ms.py --range AL AZ
```

### Scraping All States
To scrape all available states:
```bash
python ms.py --all
```

### Scraping Regulations
Add the `-r` or `--regs` flag to any of the above commands to download regulations instead of codes.
```bash
python ms.py --states CA TX -r
```

### Specifying Number of Threads
You can control the number of parallel threads with the `-t` or `--threads` flag:
```bash
python ms.py --all -t 8
```
