#!/usr/bin/env bash
# ===> COMMON SECTION START  ===>
# http://bash.cumulonim.biz/NullGlob.html
shopt -s nullglob


# ---------- CONSTANTS ----------
export FILE_VARIABLES=${FILE_VARIABLES:-".variables"}
export FILE_SECRETS=${FILE_SECRETS:-".secrets"}

# -------------------------------

# -------------------------------
# --- COMMON FUNCTION SECTION ---

debug(){
    local __msg="$@"
    echo " [DEBUG] `date` ... $__msg "
}

info(){
    local __msg="$@"
    echo " [INFO]  `date` ->>> $__msg "
}

warn(){
    local __msg="$@"
    echo " [WARN]  `date` *** $__msg "
}

err(){
    local __msg="$@"
    echo " [ERR]   `date` !!! $__msg "
}

add_entry_to_variables()
{
  info "[add_entry_to_variables|in] ($1, $2)"
  [ -z "$1" ] && err "no parameters provided" && return 1

  variables_file="${this_folder}/${FILE_VARIABLES}"

  if [ -f "$variables_file" ]; then
    sed -i '' "/export $1/d" "$variables_file"

    if [ ! -z "$2" ]; then
      echo "export $1=$2" | tee -a "$variables_file" > /dev/null
    fi
  fi
  info "[add_entry_to_variables|out]"
}

add_entry_to_secrets()
{
  info "[add_entry_to_secrets|in] ($1, ${2:0:7})"
  [ -z "$1" ] && err "no parameters provided" && return 1

  secrets_file="${this_folder}/${FILE_SECRETS}"

  if [ -f "$secrets_file" ]; then
    sed -i '' "/export $1/d" "$secrets_file"

    if [ ! -z "$2" ]; then
      echo "export $1=$2" | tee -a "$secrets_file" > /dev/null
    fi
  fi
  info "[add_entry_to_secrets|out]"
}

verify_prereqs(){
  info "[verify_prereqs] ..."
  for arg in "$@"
  do
      debug "[verify_prereqs] ... checking $arg"
      which "$arg" 1>/dev/null
      if [ ! "$?" -eq "0" ] ; then err "[verify_prereqs] please install $arg" && return 1; fi
  done
  info "[verify_prereqs] ...done."
}

verify_env(){
  info "[verify_env] ..."
  for arg in "$@"
  do
      debug "[verify_env] ... checking $arg"
      if [ -z "$arg" ]; then err "[verify_env] please define env var: $arg" && return 1; fi
  done
  info "[verify_env] ...done."
}

# -------------------------------
# --- MAIN SECTION ---

if [ -z "$this_folder" ]; then
  this_folder="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
  if [ -z "$this_folder" ]; then
    this_folder=$(dirname $(readlink -f $0))
  fi
fi
parent_folder=$(dirname "$this_folder")

if [ ! -f "$this_folder/$FILE_VARIABLES" ]; then
  warn "we DON'T have a $FILE_VARIABLES variables file"
else
  . "$this_folder/$FILE_VARIABLES"
fi

if [ ! -f "$this_folder/$FILE_SECRETS" ]; then
  warn "we DON'T have a $FILE_SECRETS secrets file"
else
  . "$this_folder/$FILE_SECRETS"
fi

usage()
{
  cat <<EOM
  usages:
  $(basename $0) {reqs}
                        reqs
                            install required packages to build and publish python packages
                        build
                            build package
                        test
                            runs unit tests
                        create_requirements
                            creates "requirements.txt" file
                        code_check
                          code check: runs 'black', 'autoflake' & 'isort'
                        publish {patch|minor|major}
                          after testing and committing everything, we may want to bumpversion,
                          tag and push the code to create e new package version


EOM
  exit 1
}

reqs()
{
    info "[reqs|in]"
    python -m pip install --upgrade pip
    pip install setuptools wheel build twine artifacts-keyring keyring bump2version pipreqs astroid pycodestyle pyflakes isort black autoflake pytest pytest-cov
    return_value="$?"
    info "[reqs|out] => ${return_value}"
    [[ ! "$return_value" -eq "0" ]] && exit 1
}

code_check()
{
    info "[code_check|in]"
    autoflake --in-place --remove-unused-variables --check -r src test && \
    isort -rc src test && \
    black src test -t py37 --line-length=120
    return_value="$?"
    if [ "$return_value" -eq "0" ]; then
      outdated_packages=$(pip list --outdated --format freeze  | wc -l | awk '{ print $1 }')
      if [ ! "$outdated_packages" -eq "0" ]; then
        err "[code_check] outdated packages: $outdated_packages"
        return_value="1"
      fi
    fi
    info "[code_check|out] => ${return_value}"
    [[ ! "$return_value" -eq "0" ]] && exit 1
}


build()
{
    info "[build|in]"
    rm -f dist/*
    pyproject-build && twine check dist/*
    return_value=$?
    info "[build|out] => ${return_value}"
    return $return_value
}

publish()
{
    info "[publish|in]"
    python -m twine upload dist/*.whl
    return_value=$?
    info "[publish|out] => ${return_value}"
    return $return_value
}

test()
{
    info "[test|in]"
    python -m pytest test
    return_value="$?"
    info "[test|out] => ${return_value}"
    [[ ! "$return_value" -eq "0" ]] && exit 1
}

create_requirements()
{
  info "[create_requirements|in]"
  pipreqs ./ --ignore .env --force
  info "[create_requirements|out]"
}

info "starting [ $0 $1 $2 $3 $4 ] ..."
_pwd=$(pwd)

case "$1" in
    reqs)
        reqs
        ;;
    build)
        build
        ;;
    publish)
        publish
        ;;
    test)
        test
        ;;
    create_requirements)
        create_requirements
        ;;
    code_check)
        code_check
        ;;
    *)
        usage
esac

info "...[ $0 $1 $2 $3 $4 ] done."