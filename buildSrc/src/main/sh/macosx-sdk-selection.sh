#!/bin/bash
#

# Get the current OS version number

macosx_version=`uname -r`

# depending on the version number, we need to set the SDK-related variables differently:

case $macosx_version in
    14*)
        SDKROOT='/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX10.10.sdk/'
        MACOSX_DEPLOYMENT_TARGET='10.10'
        ;;
    13*)
        SDKROOT='/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX10.9.sdk/'
        MACOSX_DEPLOYMENT_TARGET='10.9'
        ;;
    12*)
        SDKROOT='/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX10.8.sdk/'
        MACOSX_DEPLOYMENT_TARGET='10.8'
        ;;
    11*)
        SDKROOT='/Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX10.7.sdk/'
        MACOSX_DEPLOYMENT_TARGET='10.7'
        ;;
    *)
        SDKROOT='/Developer/SDKs/MacOSX10.6.sdk'
        MACOSX_DEPLOYMENT_TARGET='10.6'
        ;;
esac
