# mat-facebook-ads-transformer
Transformer for output data of [mat-facebook-downloader](https://github.com/zabkwak/mat-facebook-downloader) for Media Analytics Tool project.
The data can be used in [mat-facebook-ads-analyzer](https://github.com/zabkwak/mat-facebook-ads-analyzer).

## Development
### Requirements
- Python 3
- `gcc` and `g++` libraries
### Installation & test run
```bash
git clone git@github.com:zabkwak/mat-facebook-ads-transformer.git
cd mat-facebook-ads-transformer
pip install -r requirements.txt
python ./
```

## Settings
The settings are set with environment variables. 
Variable | Description | Required | Default value
:------------ | :------------- | :-------------| :-------------
`INPUT_FILE` | The filepath of the `parquet` archive from downloader. | :heavy_check_mark: | 
`OUT_DIR` | The directory where the output is stored. | :heavy_check_mark: | 
`CSV_SEPARATOR` | The separator of the input `csv` files. | :x: | `,`
`LANGUAGE` | The language for the udpipe analysis. | :x: | `cs`

## Output
The output directory contains models mentioned above and additional files.
File | Description
:------------ | :-------------
`ads_corpus.csv` |The corpus of all text tokens in the input file.
`config.csv` | Configuration.
`df_demographics_unnested.csv` | Ads grouped by demographics data (gender, age).
`df_imp.csv` | Base info about the ad.
`df_region.csv` |Ads grouped by regions.
`total_ads_per_funding.csv` | Total ads per funding.
`total_ads_per_page.csv` | Total ads per page.
`total_region.csv` | Total ads per region.

## Docker
The [image](https://github.com/zabkwak/mat-facebook-ads-transformer/pkgs/container/mat-facebook-ads-transformer) is stored in GitHub packages registry and the app can be run in the docker environment.
```bash
docker pull ghcr.io/zabkwak/mat-facebook-ads-transformer:latest
```

```bash
docker run \
--name=mat-facebook-ads-transformer \
-e 'INPUT_FILE=./input/archive.parquet' \
-e 'OUTPUT_DIR=./output' \
-v '/absolute/path/to/output/dir:/usr/src/app/output' \
-v '/absolute/path/to/input/dir:/usr/src/app/input' \
ghcr.io/zabkwak/mat-facebook-ads-transformer:latest  
```
The volumes must be set for accessing input and output data.