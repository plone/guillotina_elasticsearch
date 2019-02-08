from guillotina.interfaces import IInteraction
from guillotina.utils import get_current_request


async def build_security_query(container, request=None):
    # The users who has plone.AccessContent permission by prinperm
    # The roles who has plone.AccessContent permission by roleperm
    users = []
    roles = []

    if request is None:
        request = get_current_request()
    interaction = IInteraction(request)

    for user in interaction.participations:  # pylint: disable=E1133
        users.append(user.principal.id)
        users.extend(user.principal.groups)
        roles_dict = interaction.global_principal_roles(
            user.principal.id,
            user.principal.groups)
        roles.extend([key for key, value in roles_dict.items()
                      if value])
    # We got all users and roles
    # users: users and groups

    should_list = [{'match': {'access_roles': x}} for x in roles]
    should_list.extend([{'match': {'access_users': x}} for x in users])

    return {
        'query': {
            'bool': {
                'filter': {
                    'bool': {
                        'should': should_list,
                        'minimum_should_match': 1
                    }
                }
            }
        }
    }
