# export data form datea v2 to import it later in v3
import os
from django.contrib.auth.models import User
from datea.datea_action.models import DateaAction
import json
from django.core import serializers

def export_data():
	os.system('./manage.py dumpdata > data.json')
	f = open('data.json')
	data = f.read()
	data = json.loads(data)
	f.close()

	# get user passwords
	pws = {}
	for u in User.objects.all():
		pws[u.id] = u.password

	actions = {}
	for a in DateaAction.objects.all():
		actions[a.pk] = a

	for i in range(len(data)):
		if 'auth.user' == data[i]['model']:
			data[i]['fields']['password'] = pws[int(data[i]['pk'])]
		if 'datea_mapping.dateamapping' == data[i]['model']:
			action = actions[int(data[i]['pk'])]
			extra_fields = {
				'user': {'id': action.user.id},
				'name': action.name,
				'slug': action.slug,
				'published': action.published,
				'created': action.created.isoformat(),
				'modified': action.created.isoformat(),
				'short_description': action.short_description,
				'hashtag': action.hashtag,
				'category': {'id': action.category.id},
				'featured': action.featured,
				'action_type': action.action_type,
				'item_count': action.item_count,
				'user_count': action.user_count,
				'comment_count': action.comment_count,
				'follow_count': action.follow_count,
			}
			if action.image:
				extra_fields['image'] = {'id': action.image.id}
			else:
				extra_fields['image'] = None
			if action.end_date:
				extra_fields['end_date'] = action.end_date.isoformat()
			else:
				extra_fields['end_date'] = None
			data[i]['fields'].update(extra_fields)

	output = json.dumps(data)

	f = open('data.json', 'w')
	f.write(output)
	f.close()

