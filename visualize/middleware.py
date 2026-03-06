import base64

# from account.models import Account
# from Crypto.Cipher import AES
# from Crypto.Util.Padding import unpad
from django.shortcuts import render
from django.contrib.auth import login

AES_KEY = b"dg%$%#^#gdw786dg"
MSG_SECRET = "#$%#dtrd^%oxiuy"

# allowed_paths = [
#     "",
#     "/admin/register",
#     "/adminsite",
#     "/adminsite/login",
#     "/admin",
#     "/change_password",
#     "/logout",
# ]
# allowed_paths = allowed_paths + list(map(lambda x: x + "/", allowed_paths))

# BK5YyoqRE2osEa2SrlKc9g==;O1RURKx7tiI9GZngWanwhQ==;7utvt5+4RS+eTvursLGlpA==;RiKezqLVeiuzas8lqCh/yw==
# BK5YyoqRE2osEa2SrlKc9g%3D%3D%3BO1RURKx7tiI9GZngWanwhQ%3D%3D%3B7utvt5%2B4RS%2BeTvursLGlpA%3D%3D%3BRiKezqLVeiuzas8lqCh%2Fyw%3D%3D


# def decrypt(encrypted_string, iv):
#     cipher = AES.new(AES_KEY, AES.MODE_CBC, base64.b64decode(iv))
#     return unpad(
#         cipher.decrypt(base64.b64decode(encrypted_string)), AES.block_size
#     ).decode()


# def validate_message_secret(encrypted_msg_secret, iv):
#     # print(decrypt(encrypted_msg_secret, iv))
#     if MSG_SECRET != decrypt(encrypted_msg_secret, iv):
#         raise Exception("Message secret validation failed.")


# def CustomAuthenticationMiddleware(get_response):
#     def middleware(request):
#         try:
#             if not request.COOKIES.get("auth"):
#                 if "user" in request.GET:   
#                     auth_info = request.GET["user"]
#                     auth_info = auth_info.split(";")
#                     validate_message_secret(auth_info[1], auth_info[0])
#                     response = get_response(request)
#                     response.set_cookie("auth", ';'.join(auth_info[:2]), max_age=None)
#                     response.set_cookie("id", decrypt(auth_info[2], auth_info[0]), max_age=None)
#                     response.set_cookie("name", decrypt(auth_info[3], auth_info[0]), max_age=None)
#                     return response
#                 else:
#                     response = get_response(request)
#                     response.set_cookie("name", "User ", max_age=None)
#                     return response
#             else:
#                 auth_info = request.COOKIES.get("auth").split(";")
#                 validate_message_secret(auth_info[1], auth_info[0])
#                 return get_response(request)
#         except:
#             return render(request, "visualize/access_denied.html")

#     return middleware



