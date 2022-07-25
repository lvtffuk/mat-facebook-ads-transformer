class Range:

	min: int

	max: int

	@property
	def mid(self) -> float:
		if self.min is None or self.max is None:
			return None
		return (self.min + self.max) / 2

	def __init__(self, min: int, max: int) -> None:
		self.min = min
		self.max = max

	def to_string(self) -> str:
		return f"{self.min}-{self.max}"
