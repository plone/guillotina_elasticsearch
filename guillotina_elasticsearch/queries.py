from guillotina.utils import get_authenticated_user
from guillotina.utils import get_security_policy


async def build_security_query(container):
    # The users who has plone.AccessContent permission by prinperm
    # The roles who has plone.AccessContent permission by roleperm
    users = []
    roles = []
    user = get_authenticated_user()
    policy = get_security_policy(user)

    users.append(user.id)
    users.extend(user.groups)

    roles_dict = policy.global_principal_roles(user.id, user.groups)
    roles.extend([key for key, value in roles_dict.items() if value])

    # We got all users and roles
    # users: users and groups

    should_list = [{"match": {"access_roles": x}} for x in roles]
    should_list.extend([{"match": {"access_users": x}} for x in users])

    return {
        "query": {
            "bool": {
                "filter": [{"bool": {"should": should_list, "minimum_should_match": 1}}]
            }
        }
    }
