[CmdletBinding()]
param (
    [Parameter(Position=0)]
    [bool]
    $modfiles
)

if ($modfiles) {
    $sizes = "128", "512", "2k", "8k", "32k"

    foreach($size in $sizes) {
        python .\evaluation.py -n 4 -r 4 -f $size > out.txt
    }

} else {
    $clients = 2, 4, 8, 16
    $tot_requests = 16

    foreach ($cli in $clients) {
        $reruns = $tot_requests / $cli
        python .\evaluation.py -n $cli -r $reruns > out.txt
    }
}