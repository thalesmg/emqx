#!/usr/bin/env bash

set -euo pipefail

## Specify your organization - account name as the account identifier
SFACCOUNT=${SFACCOUNT:-myorganization-myaccount}
VERSION="3.3.2"
FILE="snowflake-odbc-${VERSION}.x86_64.deb"
URL="https://sfc-repo.snowflakecomputing.com/odbc/linux/${VERSION}/${FILE}"
SHA256="fdcf83aadaf92ec135bed0699936fa4ef2cf2d88aef5a4657a96877ae2ba232d"

if [[ -f "${FILE}" && $(sha256sum "${FILE}" | cut -f1 -d' ') == "${SHA256}" ]]; then
  echo "package already downloaded"
else
  echo "downloading package"
  wget -nc "$URL"
fi
dpkg -i "${FILE}"
apt install -f

DRIVER_PATH="/usr/lib/snowflake/odbc/lib/libSnowflake.so"

sed -i -e "s#^ODBCInstLib=.*#ODBCInstLib=$DRIVER_PATH#" /usr/lib/snowflake/odbc/lib/simba.snowflake.ini

cat >>/etc/odbcinst.init <<EOF
[ODBC Drivers]
SnowflakeDSIIDriver=Installed

[SnowflakeDSIIDriver]
APILevel=1
ConnectFunctions=YYY
Description=Snowflake DSII
Driver=$DRIVER_PATH
DriverODBCVer=03.52
SQLLevel=1
EOF

cat >>/etc/odbc.ini <<EOF
[ODBC Data Sources]
snowflake = SnowflakeDSIIDriver

[snowflake]
Driver      = $DRIVER_PATH
Description =
server      = $SFACCOUNT.snowflakecomputing.com
role        = sysadmin
EOF
