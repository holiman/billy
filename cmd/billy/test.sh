#PUT `echo -n ";alsdf;lsdf;lsdf" | base64`
#PUT `echo -n "asdf;lasdf;l" | base64`
#PUT `echo -n "ssdf;lasdf;lwefsdfa" | base64`

nc -U ./billy.ipc <<EOF
PUT `echo -n "lalala" | base64`
PUT `echo -n "one more key for the people" | base64`
PUT `echo -n "we can go all day" | base64`
PUT `echo -n "billy supports _tens_ of keys" | base64`
GET 0
GET 1
PUT `echo -n "that's right, it can _get_ them out again too" | base64`
GET 3
DEL 3
PUT `echo -n "this should be filled into slot 3" | base64`
GET 3
DEL 3
GET 5
EOF
