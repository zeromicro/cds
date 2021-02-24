function apiPost(url, body, ok, fail, ev) {
    var xhr = new XMLHttpRequest();
    xhr.onreadystatechange = function (e) {
        if (this.readyState == 4) {
            if (this.status == 200) {
                var obj = JSON.parse(this.responseText)
                // const {data} = obj
                if (ok) ok(obj)
            } else if (this.status == 401) {
                location.href = 'login.html'
                return
            } else {
                // var obj = JSON.parse(this.responseText)
                // const {code} = obj
                if (fail) fail(this.responseText)
            }
            if (ev) ev()
        }
    }
    xhr.open('POST', `api/${url}`)
    xhr.setRequestHeader('Authorization', 'Bearer ' + getCookie("auth"));
    if (!body) {
        xhr.send(null)
        return
    }
    if (typeof body != 'string') {
        xhr.setRequestHeader('Content-Type', 'application/json')
        xhr.send(JSON.stringify(body))
        return
    }
    xhr.send(body)
}

function apiGet(url, ok, fail, ev) {
    var xhr = new XMLHttpRequest();
    xhr.onreadystatechange = function (e) {
        if (this.readyState == 4) {
            if (this.status == 200) {
                var obj = JSON.parse(this.responseText)
                // const {data} = obj
                if (ok) ok(obj)
            } else if (this.status == 401) {
                location.href = 'login.html'
                return
            } else {
                // var obj = JSON.parse(this.responseText)
                // const {code} = obj
                // if (code >= 10001 && code <= 10010) {
                //     location.href = 'login.html'
                //     return
                // }
                if (fail) fail(this.responseText)
            }
            if (ev) ev()
        }
    }
    xhr.open('GET', `api/${url}`)
    xhr.setRequestHeader('Authorization', 'Bearer ' + getCookie("auth"));
    xhr.send(null)
}

function qs(params) {
    const queryString = Object.keys(params).map(key => key + '=' + params[key]).join('&')
    return `?${queryString}`
}


// galaxy-api

function apiUserLogin(body, ok, fail, ev) {
    apiPost('galaxy/user/login', body, ok, fail, ev);
}

function apiUserAdd(body, ok, fail, ev) {
    apiPost('galaxy/user/add', body, ok, fail, ev);
}

function apiUserGet(ok, fail, ev) {
    apiGet('galaxy/user/get', ok, fail, ev);
}

function apiBlackholeDmList(body, ok, fail, ev) {
    apiPost('galaxy/html/dm-list', body, ok, fail, ev);
}

function apiBlackholeDmAdd(body, ok, fail, ev) {
    apiPost('galaxy/html/dm-add', body, ok, fail, ev);
}

function apiBlackholeListTables(body, ok, fail, ev) {
    apiPost('galaxy/html/list-tables', body, ok, fail, ev);
}

function apiBlackholeListDatabases(body, ok, fail, ev) {
    apiPost('galaxy/html/list-databases', body, ok, fail, ev);
}

function apiBlackholeGenerateCreateSql(body, ok, fail, ev) {
    apiPost('galaxy/html/generate-create-sql', body, ok, fail, ev);
}

function apiBlackholeExecSql(body, ok, fail, ev) {
    apiPost('galaxy/html/exec-sql', body, ok, fail, ev);
}

function apiBlackholeDmStop(body, ok, fail, ev) {
    apiPost('galaxy/html/dm-stop', body, ok, fail, ev);
}

function apiBlackholeDmDelete(body, ok, fail, ev) {
    apiPost('galaxy/html/dm-delete', body, ok, fail, ev);
}

function apiBlackholeDmRedo(body, ok, fail, ev) {
    apiPost('galaxy/html/dm-redo', body, ok, fail, ev);
}

function apiBlackholeRtuList(body, ok, fail, ev) {
    apiPost('galaxy/html/rtu-list', body, ok, fail, ev);
}

function apiBlackholeRtuAdd(body, ok, fail, ev) {
    apiPost('galaxy/html/rtu-add', body, ok, fail, ev);
}

function apiBlackholeRtuStop(body, ok, fail, ev) {
    apiPost('galaxy/html/rtu-stop', body, ok, fail, ev);
}

function apiBlackholeRtuDelete(body, ok, fail, ev) {
    apiPost('galaxy/html/rtu-delete', body, ok, fail, ev);
}

function apiBlackholeRtuRedo(body, ok, fail, ev) {
    apiPost('galaxy/html/rtu-redo', body, ok, fail, ev);
}

function apiBlackholeConnectorList(body, ok, fail, ev) {
    apiPost(`galaxy/html/connector-list`, body, ok, fail, ev);
}

function apiBlackholeConnectorAdd(body, ok, fail, ev) {
    apiPost('galaxy/html/connector-add', body, ok, fail, ev);
}

function apiBlackholeConnectorDelete(body, ok, fail, ev) {
    apiPost('galaxy/html/connector-delete', body, ok, fail, ev);
}

function apiBlackholeDatabaseList(body, ok, fail, ev) {
    apiPost('galaxy/html/database-list', body, ok, fail, ev);
}

function apiBlackholeDefaultConfig(ok, fail, ev) {
    apiGet('galaxy/html/default-config', ok, fail, ev)
}

function apiChUserList(body, ok, fail, ev) {
    apiPost('ch-user-manager/list-ch-user', body, ok, fail, ev);
}

function apiChUserEdit(body, ok, fail, ev) {
    apiPost('ch-user-manager/modify-ch-user', body, ok, fail, ev);
}

function apiChUserAdd(body, ok, fail, ev) {
    apiPost('ch-user-manager/add-ch-user', body, ok, fail, ev);
}

function apiChUserDelete(body, ok, fail, ev) {
    apiPost('ch-user-manager/delete-ch-user', body, ok, fail, ev);
}

function apiChProxyUserList(body, ok, fail, ev) {
    apiPost('ch-user-manager/list-ch-proxy-user', body, ok, fail, ev);
}

function apiChProxyUserEdit(body, ok, fail, ev) {
    apiPost('ch-user-manager/modify-ch-proxy-user', body, ok, fail, ev);
}

function apiChProxyUserAdd(body, ok, fail, ev) {
    apiPost('ch-user-manager/add-ch-proxy-user', body, ok, fail, ev);
}

function apiChProxyUserDelete(body, ok, fail, ev) {
    apiPost('ch-user-manager/delete-ch-proxy-user', body, ok, fail, ev);
}

function apiChDatabaseList(body, ok, fail, ev) {
    apiPost('ch-user-manager/database-list', body, ok, fail, ev);
}

function apChClusterList(body, ok, fail, ev) {
    apiPost('ch-user-manager/cluster-list', body, ok, fail, ev);
}

function getQueryVariable(variable)
{
    var query = window.location.search.substring(1);
    var vars = query.split("&");
    for (var i=0;i<vars.length;i++) {
        var pair = vars[i].split("=");
        if(pair[0] == variable){return pair[1];}
    }
    return(false);
}

function getCookie(name)
{
    var arr,reg=new RegExp("(^| )"+name+"=([^;]*)(;|$)");
    if(arr=document.cookie.match(reg))
        return unescape(arr[2]);
    else
        return null;
}