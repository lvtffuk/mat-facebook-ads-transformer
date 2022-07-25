class Range:

	min: int

	max: int

	@property
	def mid(self) -> float:
		if self.min is None or self.max is None:
			return None
		return (self.min + self.max) / 2

	def __init__(self, min: int, max: int) -> None:
		if type(min) is not int and type(min) is not float:
			min = None
		if type(max) is not int and type(max) is not float:
			max = None
		self.min = min
		self.max = max

	def to_string(self) -> str:
		return f"{self.min or 0}-{self.max or 0}"
