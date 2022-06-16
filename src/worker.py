from datetime import date, datetime
from os.path import exists
import pandas as pd
from src.range import Range
import spacy_udpipe
from src.writer import Writer
from alive_progress import alive_bar


CORPUS_HEADER = [
	"doc_id",
	"paragraph_id",
	"sentence_id",
	"token_id",
	"token",
	"lemma",
	"upos",
	"xpos",
	"feats",
	"head_token_id",
	"dep_rel",
	"deps",
	"misc"
]

REGION_HEADER = ["ad_id", "percentage", "region", "page_name", "page_id"]

DEMOGRAPHIC_HEADER = [
	"ad_id", 
	"age",
	"gender",
	"percentage",
	"page_name",
	"page_id",
	"impressions_mid"
]

IMP_HEADER = [
	"ad_creation_time",
	"ad_delivery_start_time",
	"ad_delivery_stop_time",
	"ad_snapshot_url",
	"ad_creative_body",
	"page_id",
	"page_name",
	"currency",
	"spend_lower_bound",
	"spend_upper_bound",
	"funding_entity",
	"id",
	"ad_id",
	"spend_mid",
	"spend_interval",
	"lang",
]

class Worker:

	input_file: str

	output_dir: str

	lang: str = "cs"

	_regions: dict = {}

	_pages: dict = {}

	_funding: dict = {}

	_min_date: date = None

	_max_date: date = None

	_nlp = None

	def __init__(self, input_file: str, output_dir: str, lang: str = "cs") -> None:
		self.input_file = input_file
		self.output_dir = output_dir
		self.lang = lang or "cs"

	def process(self) -> None:
		self._validate_input_file()
		self._validate_output_dir()
		spacy_udpipe.download(self.lang)
		self._nlp = spacy_udpipe.load(self.lang)
		self._process_archive()

	def _process_archive(self) -> None:
		df = pd.read_parquet(self.input_file, engine="pyarrow")
		corpus_writer = Writer(self._get_output_file_path("ads_corpus.csv"), CORPUS_HEADER)
		region_writer = Writer(self._get_output_file_path("df_region.csv"), REGION_HEADER)
		demographic_writer = Writer(self._get_output_file_path("df_demographics_unnested.csv"), DEMOGRAPHIC_HEADER)
		imp_writer = Writer(self._get_output_file_path("df_imp.csv"), IMP_HEADER)
		default_date = datetime.now().strftime("%Y-%m-%d")
		with alive_bar(len(df.index)) as bar:		
			for index, row in df.iterrows():
				for t in self._process_nlp(row["ad_creative_bodies"]):
					corpus_writer.write_row(self._map_corpus_row(row["id"], t))
				for d in self._process_ad_by_region(row):
					region_writer.write_row(self._map_region_row(row["id"], d))
				for d in self._process_ad_by_demographic_data(row):
					demographic_writer.write_row(self._map_demographic_row(row["id"], d))
				spend = self._fb_range_to_range(row["spend"])
				imp_writer.write_row([
					row["ad_creation_time"],
					row["ad_delivery_start_time"] or default_date,
					row["ad_delivery_stop_time"] or default_date,
					row["ad_snapshot_url"] or "NA",
					self._get_first_list_value(row["ad_creative_bodies"]).translate(str.maketrans({ '"': r'\"' })),
					row["page_id"],
					row["page_name"],
					row["currency"] or "NA",
					str(spend.min),
					str(spend.max),
					row["bylines"],
					row["id"],
					row["id"],
					spend.mid,
					spend.to_string(),
					self._get_first_list_value(row["languages"])
				])
				self._process_dates(row["ad_creation_time"])
				self._process_pages(row["page_name"])
				self._process_funding(row["bylines"])
				self._process_regions(row["delivery_by_region"])
				bar()
		self._save_csv(
			self._get_output_file_path("total_ads_per_page.csv"),
			["page_name", "n_ads"],
			sorted(self._pages.items(), key=lambda l: l[1], reverse=True)
		)
		self._save_csv(
			self._get_output_file_path("total_ads_per_funding.csv"),
			["funding_entity", "n_ads"],
			sorted(self._funding.items(), key=lambda l: l[1], reverse=True)
		)
		self._save_csv(
			self._get_output_file_path("total_region.csv"),
			["region", "percentage"],
			sorted(self._regions.items(), key=lambda l: l[1], reverse=True)
		)
		self._save_csv(
			self._get_output_file_path("config.csv"),
			["mindate", "maxdate"],
			[[self._min_date.strftime("%Y-%m-%d"), self._max_date.strftime("%Y-%m-%d")]]
		)
		corpus_writer.close()
		region_writer.close()
		demographic_writer.close()
		imp_writer.close()

	def _process_nlp(self, data: list[str]) -> list[dict]:
		if len(data) == 0:
			return []
		text = data[0]
		sentence_id = 0
		doc = self._nlp(text)
		output = []
		for token in doc:
			if token.is_sent_start:
				sentence_id += 1
			# print(token.is_sent_start, token.text, token.lemma_, token.pos_, token.dep_)
			output.append({
				"paragraph_id": 1, # TODO?
				"sentence_id": sentence_id,
				"token_id": 0, # TODO?
				"token": token.text,
				"lemma": token.lemma_,
				"upos": token.pos_,
				"xpos": None,
				"feats": None,
				"head_token_id": None,
				"dep_rel": None,
				"deps": None,
				"misc": None
			})
		return output

	def _process_ad_by_region(self, data: dict) -> list[dict]:
		def map_f(d: dict):
			return {
				"id": data["id"],
				"percentage": d["percentage"],
				"region": d["region"],
				"page_name": data["page_name"],
				"page_id": data["page_id"]
			}
		return list(map(map_f, data["delivery_by_region"]))

	def _process_ad_by_demographic_data(self, data: dict) -> list[dict]:
		range = self._fb_range_to_range(data["impressions"])
		def map_f(d: dict):
			return {
				"id": data["id"],
				"age": d["age"],
				"gender": d["gender"],
				"percentage": d["percentage"],
				"page_name": data["page_name"],
				"page_id": data["page_id"],
				"impressions": range.mid or 0
			}
		return list(map(map_f, data["demographic_distribution"]))

	def _process_pages(self, data: str) -> None:
		name = data or "NA"
		if name in self._pages:
			self._pages[name] +=1
		else:
			self._pages[name] = 1

	def _process_funding(self, data: str) -> None:
		name = data or "NA"
		if name in self._funding:
			self._funding[name] +=1
		else:
			self._funding[name] = 1

	def _process_dates(self, data: str) -> None:
		date = datetime.strptime(data, "%Y-%m-%d")
		if not self._min_date or date < self._min_date:
			self._min_date = date
		if not self._max_date or date > self._max_date:
			self._max_date = date
					
	def _process_regions(self, data: list) -> None:
		for d in data:
			if d["region"] in self._regions:
				self._regions[d["region"]] += 1
			else:
				self._regions[d["region"]] = 1	

	def _fb_range_to_range(self, range: dict) -> Range:
		try:
			return Range(range["lower_bound"], range["upper_bound"])
		except:
			return Range(0, 0)

	def _map_corpus_row(self, doc_id: str, data: dict) -> list:
		return [
			doc_id,
			data["paragraph_id"],
			data["sentence_id"],
			data["token_id"],
			data["token"],
			data["lemma"],
			data["upos"],
			data["xpos"],
			data["feats"],
			data["head_token_id"],
			data["dep_rel"],
			data["deps"],
			data["misc"]
		]

	def _map_region_row(self, doc_id: str, data: dict) -> list:
		return [
			doc_id,
			data["percentage"], 
			data["region"], 
			data["page_name"], 
			data["page_id"]
		]

	def _map_demographic_row(self, doc_id: str, data: dict) -> list:
		return [
			doc_id,
			data["age"],
			data["gender"],
			data["percentage"],
			data["page_name"],
			data["page_id"],
			data["impressions"]
		]

	def _save_csv(self, file_path: str, header: list[str], data: list[list]) -> None:
		writer = Writer(file_path, header)
		for row in data:
			writer.write_row(row)
		writer.close()

	def _get_first_list_value(self, lst: list, default_value: str = "NA") -> str:
		if len(lst) > 0:
			return lst[0]
		return default_value

	def _get_output_file_path(self, file_name: str) -> str:
		return f"{self.output_dir}/{file_name}"

	def _validate_input_file(self) -> None:
		if not exists(self.input_file):
			raise ValueError(f"Input file '{self.input_file}' doesn't exist")

	def _validate_output_dir(self) -> None:
		if not exists(self.output_dir):
			raise ValueError(f"Output dir '{self.output_dir}' doesn't exist")
