.. contents::

PLONE.SERVER.GOOGLEOAUTH
========================

A call to plone.server using header : 

AUTHORIZATION: bearer GOOGLE_TOKEN

authorizes the user to the one in google with that token

Features
--------

 * There is no persistence information about the user

 * The configuration is global for all application


Configuration
-------------

Generic global configuration on plone.server utilities section: 

{
    "provides": "plone.server.googleoauth.oauth.IOAuth",
    "factory": "plone.server.googleoauth.oauth.OAuth",
    "settings": {
        "oauth_json": "PATH_TO_JSON_CREDENTIALS_FROM_GOOGLE",
        "credentials": "CREDENTIALS_STORAGE",
        "client": "plone"
    }
}


Installation on a site
----------------------

POST SITE_URL/@install

{
	'pluggins': [
		'plone.server.googleoauth'
	]
}

Uninstall on a site
-------------------

POST SITE_URL/@uninstall

{
	'pluggins': [
		'plone.server.googleoauth'
	]
}


Events
------

plone.server.auth.events.NewUserLogin