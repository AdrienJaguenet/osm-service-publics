def format_horaire(h):
	if h is None:
		return None
	else:
		return f"{h:%H:%M}"

def convert_day(day):
	day = day.lower()
	if day == 'lundi':
		return 'Mo'
	elif day == 'mardi':
		return 'Tu'
	elif day == 'mercredi':
		return 'We'
	elif day == 'jeudi':
		return 'Th'
	elif day == 'vendredi':
		return 'Fr'
	elif day == 'samedi':
		return 'Sa'
	elif day == 'dimanche':
		return 'Su'
	else:
		return day
