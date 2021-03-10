#!/usr/bin/env bash

# error codes
# 0 - exited without problems
# 1 - parameters not supported were used or some unexpected error occurred
# 2 - OS not supported by this script
# 3 - installed version of rclone is up to date
# 4 - supported unzip tools are not available

set -e

#when adding a tool to the list make sure to also add its corresponding command further in the script
unzip_tools_list=('unzip' '7z' 'busybox')

usage() { echo "Usage: curl https://raw.githubusercontent.com/wiserain/rclone/mod/install.sh | sudo bash" 1>&2; exit 1; }

if [ -n "$1" ]; then
    tag_name="$1"
else
    tag_name=$(curl --silent https://api.github.com/repos/wiserain/rclone/releases/latest | grep '"tag_name":' | sed -E 's/.*"([^"]+)".*/\1/')
fi


#create tmp directory and move to it with macOS compatibility fallback
tmp_dir=`mktemp -d 2>/dev/null || mktemp -d -t 'rclone-install.XXXXXXXXXX'`; cd $tmp_dir


#make sure unzip tool is available and choose one to work with
set +e
for tool in ${unzip_tools_list[*]}; do
    trash=`hash $tool 2>>errors`
    if [ "$?" -eq 0 ]; then
        unzip_tool="$tool"
        break
    fi
done  
set -e

# exit if no unzip tools available
if [ -z "${unzip_tool}" ]; then
    printf "\nNone of the supported tools for extracting zip archives (${unzip_tools_list[*]}) were found. "
    printf "Please install one of them and try again.\n\n"
    exit 4
fi

# Make sure we don't create a root owned .config/rclone directory #2127
export XDG_CONFIG_HOME=config

#check installed version of rclone to determine if update is necessary
version=`rclone --version 2>>errors | head -n 1 | cut -d ' ' -f 2`
if [ "$version" = "$tag_name" ]; then
    printf "\nThe latest version of rclone mod ${version} is already installed.\n\n"
    exit 3
fi



#detect the platform
OS="`uname`"
case $OS in
  Linux)
    OS='linux'
    echo $PREFIX | grep -qo "com.termux" && \
      { OS="termux"; unzip_tool="deb"; }
    ;;
  FreeBSD)
    OS='freebsd'
    ;;
  NetBSD)
    OS='netbsd'
    ;;
  OpenBSD)
    OS='openbsd'
    ;;  
  Darwin)
    OS='osx'
    ;;
  SunOS)
    OS='solaris'
    echo 'OS not supported'
    exit 2
    ;;
  *)
    echo 'OS not supported'
    exit 2
    ;;
esac

OS_type="`uname -m`"
case $OS_type in
  x86_64|amd64)
    OS_type='amd64'
    ;;
  i?86|x86)
    OS_type='386'
    ;;
  arm*)
    OS_type='arm'
    ;;
  aarch64)
    OS_type='arm64'
    ;;
  *)
    echo 'OS type not supported'
    exit 2
    ;;
esac


#download and unzip
rclone_zip="rclone-${tag_name}-$OS-$OS_type.zip"
[ $OS = "termux" ] && rclone_zip="rclone-${tag_name}-$OS-$(dpkg --print-architecture).deb"
download_link="https://github.com/wiserain/rclone/releases/download/${tag_name}/${rclone_zip}"

curl -LJO $download_link
unzip_dir="tmp_unzip_dir_for_rclone"
# there should be an entry in this switch for each element of unzip_tools_list
case $unzip_tool in
  'unzip')
    unzip -a $rclone_zip -d $unzip_dir
    ;;
  '7z')
    7z x $rclone_zip -o$unzip_dir
    ;;
  'busybox')
    mkdir -p $unzip_dir
    busybox unzip $rclone_zip -d $unzip_dir
    ;;
  'deb')
    mkdir -p $unzip_dir/empty
    ;;
esac
    
cd $unzip_dir/*


#mounting rclone to environment

case $OS in
  'linux')
    #binary
    cp rclone /usr/bin/rclone.new
    chmod 755 /usr/bin/rclone.new
    chown root:root /usr/bin/rclone.new
    mv /usr/bin/rclone.new /usr/bin/rclone
    #manuals
    if ! [ -x "$(command -v mandb)" ]; then
        echo 'mandb not found. The rclone man docs will not be installed.'
    else 
        mkdir -p /usr/local/share/man/man1
        cp rclone.1 /usr/local/share/man/man1/
        mandb
    fi
    ;;
  'freebsd'|'openbsd'|'netbsd')
    #bin
    cp rclone /usr/bin/rclone.new
    chown root:wheel /usr/bin/rclone.new
    mv /usr/bin/rclone.new /usr/bin/rclone
    #man
    mkdir -p /usr/local/man/man1
    cp rclone.1 /usr/local/man/man1/
    makewhatis
    ;;
  'osx')
    #binary
    mkdir -p /usr/local/bin
    cp rclone /usr/local/bin/rclone.new
    mv /usr/local/bin/rclone.new /usr/local/bin/rclone
    #manual
    mkdir -p /usr/local/share/man/man1
    cp rclone.1 /usr/local/share/man/man1/    
    ;;
  'termux')
    cd ../..
    dpkg -i $rclone_zip
    ;;
  *)
    echo 'OS not supported'
    exit 2
esac


#update version variable post install
version=`rclone --version 2>>errors | head -n 1`

printf "\n${version} has successfully installed."
printf '\nNow run "rclone config" for setup. Check https://rclone.org/docs/ for more details.\n\n'
exit 0
