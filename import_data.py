# -*- coding: utf-8 -*-

import json

from account.models import User
from campaign.models import Campaign
from category.models import Category
from comment.models import Comment
from dateo.models import Dateo, DateoStatus
from follow.models import Follow
from image.models import Image
from tag.models import Tag
from vote.models import Vote
from social.apps.django_app.default.models import UserSocialAuth

import urllib2
from urlparse import urlparse
from django.core.files import File
from dateutil.parser import parse as date_parser
from django.db import connection, transaction
import urllib
import re

from django.contrib.gis.geos import GEOSGeometry
from django.utils.timezone import utc
from datetime import datetime, timedelta

from datea_api.utils import remove_accents
from django.template.defaultfilters import slugify

tags = {}
categorias = {}
mapeos = {}
dateos = {}
imagenes = {}
votos = {}
follows = {}
comentarios = {}
respuestas = {}
users = {}
usersSocial = {}
passwords = {}

replace_users = {}

datea = 'datea.pe'


skip_mapeos = [80, 138]
replace_mapeos = {138: 139}


def get_data():
	
	global tags, categorias, mapeos, dateos, imagenes, votos, follows, comentarios, users, passwords
	jsondata = json.load(open('impdata/data.json'))

	for obj in jsondata:

		if 'free' in obj['model']:
			tags[obj['pk']] = obj['fields']

		elif 'category' in obj['model']:
			categorias[obj['pk']] = obj['fields']

		elif 'datea_mapping.dateamapping' == obj['model']:
			mapeos[obj['pk']] = obj['fields']

		elif 'datea_mapping.dateamapitem' == obj['model']:
			dateos[obj['pk']] = obj['fields']

		elif 'dateaimage' in obj['model']:
			imagenes[obj['pk']] = obj['fields']

		elif 'dateavote' in obj['model']:
			votos[obj['pk']] = obj['fields']

		elif 'dateafollow' in obj['model']:
			follows[obj['pk']] = obj['fields']

		elif 'dateacomment' in obj['model']:
			comentarios[obj['pk']] = obj['fields']

		elif 'auth.user' == obj['model']:
			users[obj['pk']] = obj['fields']

		elif 'dateaprofile' in obj['model']:
			user_pk = obj['fields']['user']
			users[user_pk]['profile'] = obj['fields']
		elif 'social_auth.usersocialauth' == obj['model']:
			user_pk = obj['fields']['user']
			usersSocial[user_pk] = obj['fields']
		elif 'datea_mapping.dateamapitemresponse' == obj['model']:
			respuestas[obj['pk']] = obj['fields']



def hashtagify(title):
	words = [x.capitalize() for x in re.split('\W+', title.strip(), flags=re.U)]
	tag = "".join(words[:6])
	replace = {
		u'ProblemasCon': u'',
		u'AccesosA': u'Acceso',
		u'ParaViajeros(restaurantes,Hoteles,Etc)/ParaOsViajantes(restaurantes,Hotéis,Etc)': u'ParaViajeros/ParaOsViajantes'
	}
	for k,v in replace.iteritems():
		tag = tag.replace(k,v)

	tag = re.sub("[\W_]", '', tag, flags=re.UNICODE)
	return tag

# Crear Usuarios primero
def create_users():
	User.objects.exclude(pk=1).delete()
	
	for pk, fields in users.iteritems():
		if pk != 1:
			if fields['email'] != '':
				existing = User.objects.filter(email = fields['email'])
				if existing.count() > 0:
					ou = existing[0]
					if ou.is_active == False and fields['is_active'] == True:
						replace_users[ou.pk] = pk
						for k,v in replace_users.iteritems():
							if v == ou.pk:
								replace_users[k] = pk
						ou.delete()
					elif ou.is_active == True and fields['is_active'] == False:
						replace_users[pk] = ou.pk
						for k,v in replace_users.iteritems():
							if v == pk:
								replace_users[k] = ou.pk
						continue
					else:
						replace_users[ou.pk] = pk
						for k,v in replace_users.iteritems():
							if v == ou.pk:
								replace_users[k] = pk
						ou.delete()

			#print "email:", fields['email']
			nu = User(pk=pk)
			nu.username = fields['username']
			if fields['email']:
				nu.email = fields['email']
				nu.status = 1
			nu.password = fields['password']
			nu.full_name = fields['profile']['full_name']
			nu.message = fields['profile']['message']
			nu.is_active = fields['is_active']
			nu.is_staff = fields['is_staff']
			nu.is_superuser = fields['is_superuser']
			nu.comment_count = fields['profile']['comment_count']
			nu.dateo_count = fields['profile']['item_count']
			nu.vote_count = fields['profile']['vote_count']
			nu.client_domain = datea
			nu.save()

			joined = date_parser(fields['date_joined'])
			lastlog = date_parser(fields['last_login'])
			User.objects.filter(pk=nu.pk).update(date_joined=joined, created=joined, last_login=lastlog)

	for pk, fields in usersSocial.iteritems():
		if fields['user'] != 1:
			nusoc = UserSocialAuth(pk=pk)
			nusoc.provider = fields['provider']
			nusoc.uid = fields['uid']
			nusoc.user_id = get_user(int(fields['user']))
			nusoc.extra_data = fields['extra_data']
			nusoc.save()

def get_user(id):

	if replace_users.has_key(id):
		return replace_users[id]
	return id

# Grabar imagenes
def create_images():

	Image.objects.all().delete()

	for pk, fields in imagenes.iteritems():

		im = Image(pk=pk)
		im.order = fields['order']
		im.user_id = get_user(fields['user'])
		im.width = fields['width']
		im.height = fields['height']

		file_name = fields['image'].split('/')[-1]
		#result = urllib.urlretrieve('http://datea.pe/media/'+fields['image'])
		im.image.save(file_name, File(open('impdata/'+fields['image'])))
		im.client_domain = datea
		im.save()
		#print "img", pk, im.pk


def connect_user_images():
	# grabar imagenes en usuarios
	for pk, fields in users.iteritems():
		if pk != 1:
			if fields['profile']['image']:
				try:
					u = User.objects.get(pk=pk)
					u.image_id = fields['profile']['image']
					u.save()
				except:
					pass
			elif fields['profile']['image_social']:
				try:
					u = User.objects.get(pk=pk)
					u.image_id = fields['profile']['image_social']
					u.save()
				except: 
					pass
 

def create_categories():

	Category.objects.all().delete()

	for pk, fields in categorias.iteritems():
		c = Category(pk=pk)
		c.name = fields['name']
		c.slug = fields['slug']
		c.description = fields['description']
		c.published = fields['active']
		c.save()


def create_tags():

	Tag.objects.all().delete()

	for pk, fields in tags.iteritems():
		tname = remove_accents(hashtagify(fields['name']))
		print "TAG", tname
		if Tag.objects.filter(tag__iexact=tname).count() == 0:
			t = Tag()
			t.tag = tname 
			t.title = fields['name'].strip()
			t.description = fields['description']
			t.client_domain = datea
			t.save()


def find_tag(former_pk):

	tag_data = tags[former_pk]
	tname = remove_accents(hashtagify(tag_data['name']))
	return Tag.objects.get(tag__iexact=tname)


def create_campaigns():

	Campaign.objects.all().delete()

	for pk, fields in mapeos.iteritems():

		if pk in skip_mapeos:
			continue
		#get extra data
		#adata = json.load(urllib2.urlopen('http://datea.pe/api/v1/mapping/'+str(pk)+'/?format=json'))

		c = Campaign(pk=pk)
		c.user_id = get_user(fields['user']['id'])
		c.name = fields['name']
		c.published = fields['published']
		c.featured = fields['featured']
		c.short_description = fields['short_description']
		c.mission = fields['mission']
		c.information_destiny = fields['information_destiny']
		c.long_description = fields['long_description']
		c.client_domain = datea
		if fields['center']:
			c.center = GEOSGeometry(fields['center'])
		if fields['boundary']:
			c.boundary = GEOSGeometry(fields['boundary'])

		if fields['end_date']:
			c.end_date = date_parser(fields['end_date'])

		if fields['image']:
			c.image_id = fields['image']['id']

		c.category_id = fields['category']['id']

		# main hashtag
		if fields['hashtag']:
			tname = remove_accents(hashtagify(fields['hashtag'].replace('#', '')))
		else:
			tname = remove_accents(hashtagify(fields['name'].replace('#', '')))

		if Campaign.objects.filter(slug=slugify(tname)).count() == 0:
			c.slug = slugify(tname)
		else:
			c.slug = slugify(fields['slug'])


		print "CAMPAIGN TAG NAME", tname
		
		existing = Tag.objects.filter(tag__iexact=tname)
		if existing.count() == 0:
			t = Tag()
			t.tag = tname 
			t.title = fields['name'].strip()
			#t.description = fields['short_description']
			t.save()
			c.main_tag_id = t.pk
		else:
			c.main_tag_id = existing[0].pk
		c.save()

		# secondary tags (m2m after save)
		for ipk in fields['item_categories']:
			tag = find_tag(ipk)
			c.secondary_tags.add(tag)

		created = date_parser(fields['created'])
		modified = date_parser(fields['modified'])

		Campaign.objects.filter(pk=c.pk).update(created=created, modified=modified)


def create_dateos():
	
	Dateo.objects.all().delete()
	DateoStatus.objects.all().delete()

	for pk, fields in dateos.iteritems():

		d = Dateo(pk=pk)
		d.status = fields['status']
		d.content = fields['content']
		d.user_id = get_user(fields['user'])
		d.address = fields['address']
		if fields['position']:
			d.position = GEOSGeometry(fields['position'])
		d.vote_count = fields['vote_count']
		d.comment_count = fields['comment_count']
		d.follow_count = fields['follow_count']
		d.campaign_id = fields['action']
		d.client_domain = datea

		cid = fields['action']
		if cid in replace_mapeos:
			cid = replace_mapeos[cid]
		campaign = Campaign.objects.get(pk=cid)

		# agregar categoria del mapeo anterior
		d.category = campaign.category

		d.save()

		for i in fields['images']:
			d.images.add(Image.objects.get(pk=i))

		# categoria -> etiquetas
		# print fields
		if fields['category']:
			new_tag = find_tag(fields['category'])
			d.tags.add(new_tag)
		d.tags.add(campaign.main_tag)

		created = date_parser(fields['created'])
		modified = date_parser(fields['modified'])
		Dateo.objects.filter(pk=d.pk).update(created=created, modified=modified)

		if d.status != 'new':
			ds = DateoStatus()
			ds.user = d.campaign.user
			ds.status = d.status
			ds.dateo = d
			ds.campaign = d.campaign
			ds.save()



def fill_tag_created():

	for t in Tag.objects.all():
		
		dateos = t.dateos.all()
		campaigns = list(t.campaigns.all())
		campaigns += list(t.campaigns_secondary.all())

		#created = datetime.utcnow().replace(tzinfo=utc)
		created = False

		for d in dateos:
			if not created or created < d.created:
				created = d.created

		for c in campaigns:
			if not created or created < c.created:
				created = c.created

		if not created:
			created = datetime.utcnow().replace(tzinfo=utc) - timedelta(days=365)

		Tag.objects.filter(pk=t.pk).update(created=created)



def create_comments():

	Comment.objects.all().delete()

	for pk, fields in comentarios.iteritems():

		c = Comment(pk=pk)
		c.comment = fields['comment']
		c.published = fields['published']
		c.user_id = get_user(fields['user'])
		c.content_object = Dateo.objects.get(pk=fields['object_id'])
		c.object_id = fields['object_id']
		c.client_domain = datea
		c.save()

		created = date_parser(fields['created'])
		Comment.objects.filter(pk=c.pk).update(created=created)


def responses_to_comments():
	for pk, fields in respuestas.iteritems():

		c = Comment()
		c.comment = fields['content']
		c.published = True
		c.content_object = Dateo.objects.get(pk=fields['map_items'][0])
		c.object_id = fields['map_items'][0]
		c.user_id = get_user(fields['user'])
		c.client_domain = datea
		c.save()

		created = date_parser(fields['created'])
		Comment.objects.filter(pk=c.pk).update(created=created)


def create_votes():

	Vote.objects.all().delete()

	for pk, fields in votos.iteritems():

		try:
			v = Vote(pk=pk)
			v.user_id = get_user(fields['user'])
			v.value = fields['value']
			v.content_object = Dateo.objects.get(pk=fields['object_id'])
			v.object_id = fields['object_id']
			v.client_domain = datea
			v.save()

			created = date_parser(fields['created'])
			Vote.objects.filter(pk=v.pk).update(created=created)

		except: 
			pass


def create_follows():

	Follow.objects.all().delete()

	for pk, fields in follows.iteritems():

		try:
			f = Follow(pk=pk)
			f.user_id = get_user(fields['user'])
			if fields['object_type'] == 'dateaaction':
				campaign = Campaign.objects.get(pk=fields['object_id'])
				f.content_object = campaign.main_tag
				f.follow_key = 'tag.'+str(campaign.main_tag.pk)
				f.object_id = campaign.main_tag.pk
				f.client_domain = datea
				f.save()
			elif fields['object_type'] == 'dateamapitem':
				dateo = Dateo.object.get(pk=fields['object_id'])
				f.content_object = dateo
				f.object_id = dateo.pk
				f.follow_key = 'dateo.'+str(dateo.pk)
				f.client_domain = datea
				f.save()
		except:
			pass


def fix_stats():

	# get rid of duplicate votes
	delete_ids = []
	for v in Vote.objects.all():
		if v.pk not in delete_ids:
			v2 = Vote.objects.filter(user_id=v.user_id, content_type_id=v.content_type_id, object_id= v.object_id).exclude(pk=v.pk)
			if v2.count() > 0:
				for dv in v2:
					delete_ids.append(dv.pk)
	print "vote delete duplicate", delete_ids
	Vote.objects.filter(pk__in=delete_ids).delete()


	# get rid of duplicate follows
	delete_ids = []
	for f in Follow.objects.all():
		if f.pk not in delete_ids:
			f2 = Follow.objects.filter(user_id=f.user_id, content_type_id=f.content_type_id, object_id= f.object_id).exclude(pk=f.pk)
			if f2.count() > 0:
				for df in f2:
					delete_ids.append(df.pk)
	print "follow delete duplicate", delete_ids
	Follow.objects.filter(pk__in=delete_ids).delete()


	# Fix Dateo stats
	for d in Dateo.objects.all():
		# comments
		d.comment_count = Comment.objects.filter(content_type__model="dateo", object_id=d.pk).count()
		# votes
		d.vote_count = Vote.objects.filter(content_type__model="dateo", object_id=d.pk).count()
		d.save()

	# Fix User stats
	for u in User.objects.all():
		u.dateo_count = Dateo.objects.filter(user=u).count()
		
		votes = 0	
		for d in Dateo.objects.filter(user=u):
			votes += d.vote_count

		u.voted_count = votes
		u.save()

	# Fix campaign stats
	# only for main tags
	for c in Campaign.objects.all():
		c.dateo_count = Dateo.objects.filter(tags=c.main_tag).count()
		c.follow_count = Follow.objects.filter(content_type__model="tag", object_id=c.main_tag.pk).count()
		comments = 0
		for d in Dateo.objects.filter(tags=c.main_tag):
			comments += d.comment_count
		c.comment_count = comments
		c.save()

	# tag stats
	for t in Tag.objects.all():
		tdateos =  t.dateos.filter(published=True)
		t.dateo_count = tdateos.count()
		t.follow_count = Follow.objects.filter(follow_key='tag.'+str(t.pk)).count()

		img_count = 0
		for d in tdateos:
			img_count += d.images.count()
		t.image_count = img_count
		t.save()



def update_db_indexes():

    cursor = connection.cursor()

    tables = ['account_user_id_seq',
	    'account_clientdomain_id_seq',
	    'account_user_groups_id_seq',
	    'account_user_user_permissions_id_seq',
	    'api_apiconfig_id_seq',
	    'auth_group_id_seq',
	    'auth_group_permissions_id_seq',
	    'auth_permission_id_seq',
	    'campaign_campaign_id_seq',
	    'campaign_campaign_layer_files_id_seq',
	    'campaign_campaign_secondary_tags_id_seq',
	    'category_category_id_seq',
	    'comment_comment_id_seq',
	    'dateo_dateo_id_seq',
	    'dateo_dateo_images_id_seq',
	    'dateo_dateo_files_id_seq',
	    'dateo_dateo_tags_id_seq',
	    'dateo_dateostatus_id_seq',
	    'dateo_redateo_id_seq',
	    'django_admin_log_id_seq',
	    'django_content_type_id_seq',
	    'django_site_id_seq',
	    'file_file_id_seq',
	    'flag_flag_id_seq',
	    'follow_follow_id_seq',
	    'image_image_id_seq',
	    'link_link_id_seq',
	    'notify_notification_id_seq', 
	    'notify_activitylog_id_seq',
	    'notify_activitylog_tags_id_seq',
	    'notify_notifysettings_id_seq',
	    'registration_registrationprofile_id_seq',
	    'social_auth_association_id_seq',
	    'social_auth_code_id_seq',
	    'social_auth_nonce_id_seq',
	    'social_auth_usersocialauth_id_seq',
	    'south_migrationhistory_id_seq', 
	    'tag_tag_id_seq',
	    'tastypie_apiaccess_id_seq',
	    'tastypie_apikey_id_seq',
	    'vote_vote_id_seq']

    for t in tables:
    	cursor.execute("SELECT setval('{table1}', (SELECT MAX(id) FROM {table2}))".format(table1=t, table2=t.replace('_id_seq', '')))
    	transaction.commit_unless_managed()


def migrate_content():

	print "import data"
	get_data()
	print "create users"
	create_users()
	print "create images"
	create_images()
	print "connect user images"
	connect_user_images()
	print "create categories"
	create_categories()
	print "create tags"
	create_tags()
	print "create campaigns"
	create_campaigns()
	print "create dateos"
	create_dateos()
	print "create comments"
	create_comments()
	print "create votes"
	create_votes()
	print "create follows"
	create_follows()
	print "update db indexes"
	update_db_indexes()
	print "responses to comments"
	responses_to_comments()










