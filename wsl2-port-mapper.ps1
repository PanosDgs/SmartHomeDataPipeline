# [WSL2]
# Get the current WSL2 VM IP address (changes on each reload)
$wsladdr = bash.exe -c "ifconfig eth0 | grep 'inet '"
$found = $wsladdr -match '\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
#$ErrorActionPreference  = "SilentlyContinue"

if ($found)
{
  $wsladdr = $matches[0];
} else {
  echo "The WSL2 IP address could not be found! Aborting.";
  exit
}

# [Ports]
# All ports you want to forward from WSL2 to localhost
$ports = @(8083,3391,5000,9000,9009,9092,9021)

# [Localhost]
$addr = '0.0.0.0'

# Map the ports from WSL2 to localhost one by one
for ($i = 0; $i -lt $ports.length; $i++)
{
  $port = $ports[$i];
  
  # Delete existing mapping
  netsh interface portproxy delete v4tov4 `
		listenport=$port `
		listenaddress=$addr > $Null
	
  # Create a new mapping
  netsh interface portproxy add v4tov4 `
		listenport=$port `
		listenaddress=$addr `
		connectport=$port `
		connectaddress=$wsladdr > $Null
}
